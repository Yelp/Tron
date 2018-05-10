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

(defn fetch! [state url params key f]
  (when-not (:req-inflight @state)
    (swap! state assoc :req-inflight true)
    (GET url
      :params params
      :handler
      #(swap! state merge {key (f %)
                           :req-inflight false
                           :error-message nil})
      :error-handler
      #(swap! state merge {:error-message (str "Failed to load " url)
                           :req-inflight false}))))

(defn setup [state]
  (secretary/set-config! :prefix "#")

  (defroute "/" []
    (swap! state merge {:view :jobs :view-title "Jobs"})
    (fetch! state "/api/jobs" {:include_job_runs 1} :jobs #(% "jobs")))

  (defroute "/configs" []
    (swap! state merge {:view :configs :view-title "Configs"})
    (fetch! state "/api" {} :api identity))

  (defroute "/config/:name" [name]
    (swap! state merge {:view :config :view-title (str "Config: " name)})
    (fetch! state "/api/config" {:name name} :config identity))

  (hook-browser-navigation!))
