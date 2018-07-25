(ns tronweb2.views.actionrun
  (:require [tronweb2.util :refer [format-time format-duration]]))

(defn actionrun-history [ar even]
  [:a.row.mb-1.text-dark
   {:key (ar "id")
    :class (if even "bg-light" "bg-white")
    :href (str "#/job/" (ar "job_name") "/" (ar "run_num"))}
   [:div.col (ar "run_num")]
   [:div.col (ar "state")]
   [:div.col (get-in ar ["node" "name"])]
   [:div.col (ar "exit_status")]
   [:div.col.small (format-time (ar "start_time"))]
   [:div.col.small (format-duration (ar "duration"))]])

(defn actionrun [ar history]
  [:div.container
   [:div.row.border.mb-3
    [:div.col
     [:div.row [:h5.col.p-3.bg-dark.text-light "Details"]]
     [:div.row [:div.col "State"] [:div.col (ar "state")]]
     [:div.row [:div.col "Node"] [:div.col (get-in ar ["node" "name"])]]
     [:div.row [:div.col "Raw command"] [:div.col (ar "raw_command")]]
     [:div.row [:div.col "Command"] [:div.col (ar "command")]]
     [:div.row [:div.col "Exit codes"]
      [:div.col
       (ar "exit_status")
       (if (seq (ar "exit_statuses"))
         [:div.small
          "(failed attempts: "
          (str (reverse (ar "exit_statuses")))
          ")"])]]
     [:div.row [:div.col "Start time"]
      [:div.col (format-time (ar "start_time"))]]
     [:div.row [:div.col "End time"]
      [:div.col (format-time (ar "end_time"))]]
     [:div.row [:div.col "Duration"]
      [:div.col (format-duration (ar "duration"))]]]]
   [:div.row.border.mb-3
    [:div.col
     [:div.row [:h5.col.p-3.bg-dark.text-light "Stdout"]]
     [:div.row [:div.col [:pre [:code (ar "stdout")]]]]]]
   [:div.row.border.mb-3
    [:div.col
     [:div.row [:h5.col.p-3.bg-dark.text-light "Stderr"]]
     [:div.row [:div.col [:pre [:code (ar "stderr")]]]]]]
   [:div.row.border.mb-3
    [:div.col
     [:div.row [:h5.col.p-3.bg-dark.text-light "History"]]
     [:div.row.mb-1
      [:div.col.h6 "Run"]
      [:div.col.h6 "State"]
      [:div.col.h6 "Node"]
      [:div.col.h6 "Exit"]
      [:div.col.h6 "Start"]
      [:div.col.h6 "Duration"]]
     (map actionrun-history history (cycle [true false]))]]])

(defn view [state]
  (let [jr (or (:jobrun state) {})
        runs (or (jr "runs") [])
        an (:actionrun-name state)
        ar (first (filter #(= an (% "action_name")) runs))
        ah (or (:actionrun-history state) [])]
    (if ar
      (actionrun ar ah)
      [:div.container "Loading..."])))
