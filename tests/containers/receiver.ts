// The external API: counts POST /tick by idempotency key; GET /stats.
import http from "node:http";

const byKey = new Map<string, number>();
let requests = 0;
http.createServer((req, res) => {
  req.resume();
  req.on("end", async () => {
    if (req.method === "POST" && req.url === "/tick") {
      requests++;
      const key = String(req.headers["idempotency-key"] ?? "");
      byKey.set(key, (byKey.get(key) ?? 0) + 1);
      // slow enough that effects are in flight when a replica is killed
      await new Promise((r) => setTimeout(r, 20 + Math.random() * 60));
      res.writeHead(200, { "content-type": "application/json" });
      return res.end("{}");
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ requests, keys: [...byKey.keys()] }));
  });
}).listen(8080);
