(defproject tronweb2 "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.9.908"]
                 [reagent "0.7.0"]
                 [secretary "1.2.3"]
                 [cljs-ajax "0.7.3"]]

  :plugins [[lein-cljsbuild "1.1.5"]]
  :min-lein-version "2.5.3"
  :source-paths ["src"]
  :clean-targets ^{:protect false} ["resources/public/js/compiled" "target"]

  :profiles
  {:dev
   {:dependencies [[binaryage/devtools "0.9.4"]]}
   :prod {}}

  :cljsbuild
  {:builds
   [{:id           "dev"
     :source-paths ["src"]
     :compiler     {:main                 tronweb2.core
                    :output-to            "resources/public/js/compiled/app.js"
                    :output-dir           "resources/public/js/compiled/out"
                    :asset-path           "js/compiled/out"
                    :source-map-timestamp true
                    :preloads             [devtools.preload]
                    :external-config      {:devtools/config {:features-to-install :all}}}}

    {:id           "min"
     :source-paths ["src"]
     :compiler     {:main            tronweb2.core
                    :output-to       "resources/public/js/compiled/app.js"
                    :optimizations   :advanced
                    :closure-defines {goog.DEBUG false}
                    :pretty-print    false}}]})
