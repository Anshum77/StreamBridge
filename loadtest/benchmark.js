import http from 'k6/http';
import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Configure the load test scenarios
export const options = {
  scenarios: {
    // 50 Concurrent VUs publishing events non-stop for 30s
    publishers: {
      executor: 'constant-vus',
      vus: 100,
      duration: '30s',
      startTime: '5s',
      exec: 'publish',
    },
    // 500 Concurrent WebSockets listening to the events (Connect exactly once)
    subscribers: {
      executor: 'per-vu-iterations',
      vus: 500,
      iterations: 1,
      startTime: '0s',
      maxDuration: '35s',
      exec: 'subscribe',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // HTTP errors should be less than 1%
    publish_latency: ['p(95)<50', 'p(99)<100'], // 95% of publishes should be <50ms
    delivery_latency: ['p(95)<100', 'p(99)<200'], // End-to-end streaming latency
  },
};

const publishLatency = new Trend('publish_latency');
const deliveryLatency = new Trend('delivery_latency');
const failedRequests = new Counter('failed_requests');
const wsMessagesReceived = new Counter('ws_messages_received');

// setup() runs once at the beginning of the test
export function setup() {
  const adminKey = __ENV.ADMIN_API_KEY || 'super-secret-admin-key';
  const baseUrl = 'http://localhost:8080';

  console.log(`Using Admin API Key: ${adminKey}`);

  // 1. Provision a Tenant with massive limits for the load test
  const tenantRes = http.post(`${baseUrl}/admin/tenants`, JSON.stringify({
    name: 'Load Test Corp',
    channel_limit: 1000,
    ws_limit: 10000,
    rate_limit: 100000, // 100k requests
    rate_window: 60     // per minute
  }), { 
    headers: { 'Authorization': `Bearer ${adminKey}`, 'Content-Type': 'application/json' } 
  });

  if (tenantRes.status !== 201) {
    throw new Error(`Failed to create tenant: ${tenantRes.status} ${tenantRes.body}`);
  }
  const tenantId = JSON.parse(tenantRes.body).id;

  // 2. Generate API Key
  const keyRes = http.post(`${baseUrl}/admin/tenants/${tenantId}/keys`, null, {
    headers: { 'Authorization': `Bearer ${adminKey}` }
  });
  if (keyRes.status !== 201) {
    throw new Error(`Failed to generate API key: ${keyRes.status} ${keyRes.body}`);
  }
  const apiKey = JSON.parse(keyRes.body).api_key;

  // 3. Create a single Channel to focus the fan-out load
  const channelRes = http.post(`${baseUrl}/channels`, JSON.stringify({
    name: 'benchmark-channel'
  }), { 
    headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' } 
  });
  
  if (channelRes.status !== 201) {
    throw new Error(`Failed to create channel: ${channelRes.status} ${channelRes.body}`);
  }
  const channel = JSON.parse(channelRes.body);

  // 3. Warm up the server (Postgres, Redis, Go Scheduler)
  for (let i = 0; i < 100; i++) {
    http.post(`${baseUrl}/channels/${channel.id}/events`, JSON.stringify({
      payload: {
        timestamp: Date.now(),
        sensor: "warmup",
        value: i
      }
    }), {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      }
    });
  }

  return { 
    channelId: channel.id,
    apiKey,
    baseUrl
  };
}

// publish() is executed by the 'publishers' scenario
export function publish(data) {
  const url = `${data.baseUrl}/channels/${data.channelId}/events`;
  const payload = JSON.stringify({
    payload: {
      timestamp: Date.now(),
      sensor: "k6_load_test",
      value: Math.random()
    }
  });

  const res = http.post(url, payload, {
    headers: {
      'Authorization': `Bearer ${data.apiKey}`,
      'Content-Type': 'application/json',
    },
  });

  publishLatency.add(res.timings.duration);
  
  const success = check(res, {
    'published successfully': (r) => r.status === 201,
  });
  
  if (!success) {
    failedRequests.add(1);
  }
  
  // Throttle to create steady pressure instead of an infinite tight loop.
  // 100 VUs * 100 req/s = ~10,000 req/s target
  sleep(0.01); 
}

// subscribe() is executed by the 'subscribers' scenario
export function subscribe(data) {
  // Stagger connections over 3 seconds to avoid TCP connection refused (connectex) errors
  // caused by a thundering herd of 500 instantaneous handshakes on Windows.
  sleep(Math.random() * 3);

  const wsBaseUrl = data.baseUrl.replace(/^http/, 'ws');
  const url = `${wsBaseUrl}/channels/${data.channelId}/ws`;
  
  const params = {
    headers: {
      'Authorization': `Bearer ${data.apiKey}`
    }
  };

  const res = ws.connect(url, params, function (socket) {
    socket.on('open', function () {
      // Connection established successfully
    });

    socket.on('message', function (msg) {
      try {
        const event = JSON.parse(msg);
        
        // Calculate true end-to-end delivery latency (from POST to WS receipt)
        if (event && event.payload && event.payload.timestamp) {
          deliveryLatency.add(Date.now() - event.payload.timestamp);
        }
        
        wsMessagesReceived.add(1);
      } catch (_) {
        // Ignore malformed payloads to prevent crashing the VU
      }
    });

    socket.on('error', function (e) {
      const err = e.error();
      if (err.includes("1005") || err.includes("1006")) {
        return;
      }
      console.log('An unexpected error occured: ', err);
    });

    // Close slightly before the 30s scenario ends to avoid dirty teardowns
    socket.setTimeout(() => socket.close(), 29500); 
  });

  if (!res || res.status !== 101) {
    console.log(`WS connect failed. Status=${res ? res.status : "nil"}, Body=${res ? res.body : "nil"}, Error=${res ? res.error : "nil"}`);
  }

  check(res, { 'WS connected': (r) => r && r.status === 101 });
}
