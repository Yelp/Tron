(ns tronweb2.views.job
  (:require [tronweb2.util :refer [format-time format-duration
                                   parse-in-local-time str->duration]]
            [cljs-time.core :as t]
            [cljs-time.coerce :as tc]))

(defn job-run [jr even]
  [:a.row.mb-1.text-dark
   {:key (jr "run_num")
    :class (if even "bg-light" "bg-white")
    :href (str "#/job/" (jr "job_name") "/" (jr "run_num"))}
   [:div.col (jr "run_num")]
   [:div.col (jr "state")]
   [:div.col (get-in jr ["node" "name"])]
   [:div.col.small (format-time (jr "run_time"))]
   [:div.col.small (format-time (jr "start_time"))]
   [:div.col.small (format-time (jr "end_time"))]
   [:div.col.small (format-duration (jr "duration"))]])

(defn job-ag-node [[node deps]]
  [:div.d-inline-flex.flex-row.pl-1.mb-2
   {:key node
    :class (if (> (count deps) 1) "border" "")}
   [:div.align-top.pr-2 node]
   (when (> (count deps) 0)
     (list [:div.align-top.pr-2 {:key 1} "←"]
           [:div.align-top
            {:key 2 :class (if (> (count deps) 1) "border-left" "")}
            (map job-ag-node deps)]
           [:div.w-100 {:key 3} ""]))])

(defn job-ag [jobs]
  [:div.col
   (let [jobs-map (into {} (map #(do [(% "name") (% "dependent")]) jobs))
         in-deps (fn [n nd] (some #(= n %) (last nd)))
         deps-of (fn [n] (map first (filter #(in-deps n %) jobs-map)))
         build-dag (fn f [n] [n (map f (deps-of n))])
         root-nodes (map first (filter #(= [] (last %)) jobs-map))
         dag (map build-dag root-nodes)]
     (map job-ag-node dag))])

(defn non-overlap-timelines [runs]
  (reduce
   (fn [tls {st "start_time" dr "duration" :as run}]
     (let [time (parse-in-local-time st)
           ms (str->duration dr)
           period (t/Period. 0 0 0 0 0 0 0 ms)
           interval (t/interval time (t/plus time period))
           non-ol-tls (filter #(t/overlap interval (last (first %)))
                              (map-indexed vector tls))
           idx (or (get-in non-ol-tls [0 0]) 0)]
       (update-in tls [idx] (fnil conj []) (assoc run :interval interval))))
   []
   (filter
    #(not= "" (apply str (vals (select-keys % ["start_time" "duration"]))))
    (reverse runs))))

(defn job-timeline [runs]
  (let [tls (non-overlap-timelines runs)
        intervals (map :interval (mapcat identity tls))
        gstart (apply min (map :start intervals))
        gend (apply max (map :end intervals))
        [gstart gend] (map tc/to-long [gstart gend])
        gduration (- gend gstart)]
    (for [[idx runs] (map-indexed vector tls)]
      [:div.row {:key idx}
       (for [run runs
             :let [{{:keys [start end]} :interval} run
                   [start end] (map tc/to-long [start end])
                   duration (- end start)]]
         [:div.col.m-0.p-0.position-absolute.text-light.small
          {:key (run "run_num")
           :style {:height "2rem"
                   :width (str (* 100 (/ duration gduration)) "%")
                   :top "0"
                   :left (str (* 100 (/ (- start gstart) gduration)) "%")}
           :class (case (run "state")
                    "scheduled" "bg-secondary"
                    "running" "bg-warning"
                    "succeeded" "bg-success"
                    "failed" "bg-danger"
                    "")}])])))

(defn job [j]
  [:div.container
   [:div.row.mb-3
    [:div.col.border.mr-3
     [:h5.row.p-3.bg-dark.text-light "Details"]
     [:div.row [:div.col "Status"] [:div.col (j "status")]]
     [:div.row [:div.col "Node pool"]
      [:div.col (get-in j ["node_pool" "name"])]]
     [:div.row [:div.col "Schedule"] [:div.col (j "schedule")]]
     [:div.row [:div.col "Settings"] [:div.col "-"]]
     [:div.row [:div.col "Last run"] [:div.col (format-time (j "last_run"))]]
     [:div.row [:div.col "Next run"] [:div.col (format-time (j "next_run"))]]]
    [:div.col.border.ml-3
     [:h5.row.p-3.bg-dark.text-light "Action graph"]
     (job-ag (j "action_graph"))]]
   [:div.row.border.border-bottom-0
    [:div.col.p-3.mb-0.bg-dark.text-light.h5 "Timeline"]]
   [:div.row.border.border-top-0.mb-3
    [:div.col.p-3 (job-timeline (j "runs"))]]
   [:div.row.mb-3.border.border-bottom-0
    [:div.col.p-3.mb-0.bg-dark.text-light.h5 "Job runs"]]
   [:div.row.border.border-top-0
    [:div.col.p-3
     [:div.row.mb-1
      [:div.col.h6 "Id"]
      [:div.col.h6 "State"]
      [:div.col.h6 "Node"]
      [:div.col.h6 "Scheduled"]
      [:div.col.h6 "Start"]
      [:div.col.h6 "End"]
      [:div.col.h6 "Duration"]]
     (map job-run (j "runs") (cycle [true false]))]]])

(defn view [state]
  (if-let [job-data (:job state)]
    (job job-data)
    [:div.container "Loading..."]))
