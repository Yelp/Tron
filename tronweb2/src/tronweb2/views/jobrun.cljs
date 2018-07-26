(ns tronweb2.views.jobrun
  (:require [tronweb2.util :refer [format-time format-duration]]
            [tronweb2.views.job :refer [job-ag]]))

(defn jobrun-ar [ar even]
  [:a.row.mb-1.text-dark
   {:key (ar "id")
    :class (if even "bg-light" "bg-white")
    :href (str "#/job/" (ar "job_name") "/" (ar "run_num") "/" (ar "action_name"))}
   [:div.col (ar "action_name")]
   [:div.col (ar "state")]
   [:div.col (ar "command")]
   [:div.col (get-in ar ["node" "name"])]
   [:div.col.small (format-time (ar "start_time"))]
   [:div.col.small (format-duration (ar "duration"))]])

(defn view [state]
  (if-let [jr (:jobrun state)]
    [:div.container
     [:div.row.mb-3
      [:div.col.border.mr-3
       [:h5.row.p-3.bg-dark.text-light "Details"]
       [:div.row [:div.col "State"] [:div.col (jr "state")]]
       [:div.row [:div.col "Node pool"]
        [:div.col (get-in jr ["node" "name"])]]
       [:div.row [:div.col "Scheduled"]
        [:div.col (format-time (jr "run_time"))]]
       [:div.row [:div.col "Start"]
        [:div.col (format-time (jr "start_time"))]]
       [:div.row.pb-2
        [:div.col "End"]
        [:div.col (format-time (jr "end_time"))]]]
      [:div.col.border.ml-3
       [:h5.row.p-3.bg-dark.text-light "Action graph"]
       [job-ag (jr "action_graph") nil]]]
     [:div.row.border.border-bottom-0
      [:div.col.p-3.mb-0.bg-dark.text-light.h5 "Action runs"]]
     [:div.row.border.border-top-0
      [:div.col.p-3
       [:div.row.mb-1
        [:div.col.h6 "Name"]
        [:div.col.h6 "State"]
        [:div.col.h6 "Command"]
        [:div.col.h6 "Node"]
        [:div.col.h6 "Start"]
        [:div.col.h6 "Duration"]]
       (map jobrun-ar (jr "runs") (cycle [true false]))]]]
    [:div.container "Loading..."]))
