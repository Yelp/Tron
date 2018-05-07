(ns tronweb2.routes
  (:require-macros [secretary.core :refer [defroute]])
  (:import goog.History)
  (:require [secretary.core :as secretary]
            [goog.events :as gevents]
            [goog.history.EventType :as EventType]
            [ajax.core :refer [GET]]))

(defn hook-browser-navigation! []
  (doto (History.)
    (gevents/listen
     EventType/NAVIGATE
     (fn [event]
       (secretary/dispatch! (.-token event))))
    (.setEnabled true)))

(defn setup [state]
  (secretary/set-config! :prefix "#")

  (defroute "/" []
    (.log js/console "nav to /")
    (swap! state assoc :view :jobs)
    (GET "/api/jobs"
      :params {:include_job_runs 1}
      :handler #(swap! state assoc :jobs (% "jobs"))))

  (hook-browser-navigation!))
