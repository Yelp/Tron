(ns tronweb2.views.schedule)

(defn schedule [data]
  [:div.col
    [:div.row
      [:div.col "Name"]
      [:div.col "Status"]
      [:div.col "Schedule"]
      [:div.col "Node Pool"]
      [:div.col "Last Success"]
      [:div.col "Next Run"]]
    (for [job data]
      [:a {:href (str "#/job/" (job "name"))}
        [:div.row
          [:div.col (job "name")]
          [:div.col (job "status")]
          [:div.col (let [s (job "scheduler")] (str (s "type") ": " (s "value")))]
          [:div.col ((job "node_pool") "name")]
          [:div.col (job "last_success")]
          [:div.col (or (job "next_run") "-")]]])])

(defn view [state]
  (if-let [schedule-data (:schedule state)]
    [schedule schedule-data]
    [:div.container "Loading..."]))
