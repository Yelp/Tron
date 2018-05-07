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

(defn fetch-jobs! [state]
  (when-not (:jobs-inflight @state)
    (swap! state assoc :jobs-inflight true)
    (GET "/api/jobs"
      :params {:include_job_runs 1}
      :handler
      #(swap! state merge {:jobs (% "jobs")
                           :jobs-inflight false
                           :error-message nil})
      :error-handler
      #(swap! state merge {:error-message "Failed to load jobs"
                           :jobs-inflight false}))))

(defn fetch-api! [state]
  (when-not (:api-inflight @state)
    (swap! state assoc :api-inflight true)
    (GET "/api"
      :handler
      #(swap! state merge {:api (% "api")
                           :api-inflight false
                           :error-message nil})
      :error-handler
      #(swap! state merge {:error-message "Failed to load api"
                           :api-inflight false}))))


(defn setup [state]
  (secretary/set-config! :prefix "#")

  (defroute "/" []
    (swap! state merge {:view :jobs})
    (fetch-jobs! state))

  (defroute "/configs" []
    (swap! state merge {:view :configs})
    (fetch-api! state))

  (hook-browser-navigation!))
