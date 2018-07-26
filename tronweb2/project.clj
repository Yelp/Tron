(defproject tronweb2 "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.10.238"]
                 [reagent "0.7.0"]
                 [secretary "1.2.3"]
                 [cljs-ajax "0.7.3"]
                 [clojure-humanize "0.2.2"]
                 [com.andrewmcveigh/cljs-time "0.5.2"]
                 [alanlcode/dagre "0.7.5-fork-0"]
                 [cljsjs/d3 "4.12.0-0"]]

  :plugins [[lein-cljsbuild "1.1.5"]
            [lein-figwheel "0.5.16"]]
  :min-lein-version "2.5.3"
  :source-paths ["src"]
  :clean-targets ^{:protect false} ["resources/public/js/compiled" "target"]

  :profiles
  {:dev {:dependencies [[binaryage/devtools "0.9.4"]]}
   :min {}}

  :cljsbuild
  {:builds
   [{:id           "dev"
     :source-paths ["src"]
     :figwheel     {:on-jsload "tronweb2.core/reload"}
     :compiler     {:main                 tronweb2.core
                    :output-to            "resources/public/js/compiled/app.js"
                    :output-dir           "resources/public/js/compiled/out"
                    :asset-path           "js/compiled/out"
                    :source-map-timestamp true
                    :preloads             [devtools.preload]
                    :closure-defines      {tronweb2.routes/api-uri "http://localhost:8089"}
                    :external-config      {:devtools/config {:features-to-install :all}}
                    :foreign-libs         [{:file "d3-timelines.js"
                                            :provides ["timelines"]}]}}


    {:id           "min"
     :source-paths ["src"]
     :compiler     {:main            tronweb2.core
                    :output-to       "resources/public/js/compiled/app.js"
                    :optimizations   :advanced
                    :closure-defines {goog.DEBUG false}
                    :pretty-print    false}}]})
