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

(defn fetch! [state url params key extract-fn]
  (GET url
    :params params
    :handler
    #(swap! state merge {key (extract-fn %)
                          :error-message nil})
    :error-handler
    #(swap! state merge {:error-message (str "Failed to load " url)})))

(defn setup [state]
  (secretary/set-config! :prefix "#")

  (defroute "/" []
    (swap! state merge {:view :jobs :view-title "Jobs"})
    (fetch! state "/api/jobs" {:include_job_runs 1} :jobs #(% "jobs")))

  (defroute "/configs" []
    (swap! state merge {:view :configs :view-title "Configs"})
    (fetch! state "/api" {} :api identity))

  (defroute "/config/:name" [name]
    (swap! state merge {:view :config :view-title (str "Config: " name) :config nil})
    (fetch! state "/api/config" {:name name} :config identity))

  (defroute "/job/:name" [name]
    (swap! state merge {:view :job :view-title (str "Job: " name) :job nil})
    (fetch! state (str "/api/jobs/" name) {:include_action_graph 1} :job identity))

  (defroute "/job/:name/:run" [name run]
    (swap! state merge {:view :jobrun :view-title (str "Job run: " name) :jobrun nil})
    (fetch! state
            (str "/api/jobs/" name "/" run)
            {:include_action_graph 1
             :include_action_runs 1}
            :jobrun identity))

  (defroute "/job/:name/:run/:aname" [name run aname]
    (let [{{sname "job_name" srun "run_num"} :jobrun} @state]
      (when-not (= [name run] (mapv str [sname srun]))
        (swap! state assoc :jobrun nil)
        (fetch! state
                (str "/api/jobs/" name "/" run)
                {:include_action_graph 1
                 :include_action_runs 1}
                :jobrun identity)))

    (fetch! state (str "/api/jobs/" name "/" aname) {} :actionrun-history identity)

    (swap! state merge {:view :actionrun
                        :view-title (str "Action run: " name "." aname)
                        :actionrun-name aname}))

  (hook-browser-navigation!))
