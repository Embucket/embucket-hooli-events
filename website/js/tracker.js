// Snowplow tracker initialization and event helpers.
// Set COLLECTOR_ENDPOINT to the ALB DNS name after deployment.
var COLLECTOR_ENDPOINT = "http://hooli-events-alb-1641300827.us-east-2.elb.amazonaws.com";

;(function(p,l,o,w,i,n,g){if(!p[i]){p.GlobalSnowplowNamespace=p.GlobalSnowplowNamespace||[];
p.GlobalSnowplowNamespace.push(i);p[i]=function(){(p[i].q=p[i].q||[]).push(arguments)};
p[i].q=p[i].q||[];n=l.createElement(o);g=l.getElementsByTagName(o)[0];n.async=1;
n.src=w;g.parentNode.insertBefore(n,g)}}(window,document,"script",
"https://cdn.jsdelivr.net/npm/@snowplow/javascript-tracker@3.24.3/sp.min.js","snowplow"));

window.snowplow("newTracker", "hooli", COLLECTOR_ENDPOINT, {
  appId: "hooli-events",
  platform: "web",
  contexts: {
    webPage: true,
    performanceTiming: true
  }
});

window.snowplow("enableActivityTracking", {
  minimumVisitLength: 10,
  heartbeatDelay: 10
});

window.snowplow("trackPageView");

function trackSearch(query) {
  window.snowplow("trackStructEvent", {
    category: "search",
    action: "submit",
    label: query
  });
}

function trackFilter(filterName) {
  window.snowplow("trackStructEvent", {
    category: "filter",
    action: "apply",
    label: filterName
  });
}

function trackAddToCart(eventName, price) {
  window.snowplow("trackStructEvent", {
    category: "cart",
    action: "add_to_cart",
    label: eventName,
    value: price
  });
}

function trackRemoveFromCart(eventName, price) {
  window.snowplow("trackStructEvent", {
    category: "cart",
    action: "remove",
    label: eventName,
    value: price
  });
}

function trackPurchase(orderId, total) {
  window.snowplow("trackStructEvent", {
    category: "ecommerce",
    action: "purchase",
    label: orderId,
    value: total
  });
}

function trackSignup(method) {
  window.snowplow("trackStructEvent", {
    category: "user",
    action: "signup",
    label: method
  });
}

function trackLogin(method) {
  window.snowplow("trackStructEvent", {
    category: "user",
    action: "login",
    label: method
  });
}
