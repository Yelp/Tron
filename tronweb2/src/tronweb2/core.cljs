(ns tronweb2.core
  (:require [reagent.core :as reagent]
            [tronweb2.routes :as routes]))

(defn root []
  [:div "hello"])

(defn mount-root []
  (reagent/render [root]
                  (.getElementById js/document "app")))

(defn ^:export init []
  (enable-console-print!)
  (routes/setup)
  (mount-root))
