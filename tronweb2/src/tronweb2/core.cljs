(ns tronweb2.core
  (:require [reagent.core :as reagent]
            [tronweb2.routes :as routes]
            [tronweb2.views.root :as root]))

(defonce app-state (reagent/atom {}))
(defn root-view []
  (root/view @app-state))

(defn ^:export init []
  (enable-console-print!)
  (routes/setup app-state)
  (reagent/render [root-view] (.getElementById js/document "app")))

(defn ^:export reload []
  (reagent/render [root-view] (.getElementById js/document "app"))
  (reagent/force-update-all))
