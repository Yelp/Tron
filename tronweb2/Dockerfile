FROM clojure:alpine
ADD project.clj /tmp
RUN lein deps && (lein cljsbuild once || true)
