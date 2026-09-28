# An application image that installs pg_txn from its npm package. The build
# context is staged by horizontal.test.ts: pkgs/*.tgz and app.ts.
ARG BASE=node:24-slim
FROM ${BASE}
WORKDIR /app
COPY pkgs /pkgs
RUN echo '{"name":"app","private":true,"type":"module"}' > package.json \
 && npm install --no-audit --no-fund --omit=dev /pkgs/pg-txn-client-*.tgz pg@8 >/dev/null \
 && rm -rf /pkgs
COPY app.ts app.ts
USER node
CMD ["node", "app.ts"]
