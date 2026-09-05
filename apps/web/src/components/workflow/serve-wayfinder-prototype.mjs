// Throwaway, dependency-free host for the Workflow layout comparison.
// Run: node apps/web/src/components/workflow/serve-wayfinder-prototype.mjs
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";

const fragment = await readFile(
  new URL("./wayfinder.prototype.html", import.meta.url),
  "utf8",
);
const page = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>T3 Code · Workflow prototype</title>
  <style>
    :root { color-scheme: light dark; }
    body { margin: 0; padding: 24px; background: light-dark(#ededf0, #0f0f10); }
    #t3-wayfinder-prototype { max-width: 1280px; margin: 0 auto; }
    @media(max-width: 600px) { body { padding: 8px; } }
  </style>
</head>
<body>${fragment}</body>
</html>`;

const server = createServer((request, response) => {
  const path = new URL(request.url, "http://localhost").pathname;
  if (path !== "/" && path !== "/workflow-prototype") {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Open /workflow-prototype?variant=A");
    return;
  }
  response.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(page);
});

server.listen(Number(process.argv[2] ?? 4317), "127.0.0.1", () => {
  console.log(
    `Workflow prototype: http://127.0.0.1:${server.address().port}/workflow-prototype?variant=A`,
  );
});
