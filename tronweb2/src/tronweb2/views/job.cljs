(ns tronweb2.views.job
  (:require [tronweb2.util :refer [format-time format-duration
                                   parse-in-local-time str->duration]]
            [cljs-time.core :as t]
            [cljs-time.coerce :as tc]
            [clojure.string :as str]
            [alanlcode.dagre]))

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

(defn job-ag [ag]
  (let [g (js/dagre.graphlib.Graph.)]
    (.setGraph g #js {})
    (.setDefaultEdgeLabel g #(do #js {}))
    (dorun
      (for [{name "name"} ag]
        (.setNode g name #js {"label" name "width" 150 "height" 50})))
    (dorun
      (for [{parent "name" children "dependent"} ag
            child children]
        (.setEdge g parent child)))
    (.layout js/dagre g)
    (let [all-x (mapv #(.-x (.node g %)) (.nodes g))
          all-y (mapv #(.-y (.node g %)) (.nodes g))
          max-x (+ (apply max all-x) 50)
          min-x (- (apply min all-x) 150)
          max-y (+ (apply max all-y))
          min-y (- (apply min all-x) 50)]
      [:div.col.p-0.m-0
        [:svg {:style {:border "0px"
                       :stroke-width 2
                       :background "white"
                       :width "400px"
                       :height "400px"}
               :viewBox (str min-x " " min-y " " (+ 150 max-x) " " max-y)}
          [:defs
            [:marker {:id "head" :orient "auto"
                      :markerWidth 10 :markerHeight 10
                      :refX 3 :refY 3}
              [:path {:d "M0,0 V6 L3,3 Z" :fill "black"}]]]
          (for [node-id (.nodes g)]
            (let [node (js->clj (.node g node-id))]
              [:g {:key node-id}
                [:rect (merge node {:fill "white" :stroke "black"
                                    "y" (- (node "y") (/ (node "height") 2))
                                    "x" (- (node "x") (/ (node "width") 2))})]
                [:text (merge node {:text-anchor "middle"
                                    :alignment-baseline "central"
                                    :style {:font-size "25px"}})
                       (node "label")]]))
          (for [edge-id (.edges g)]
            (let [edge (.edge g edge-id)
                  [p1 p2 & prest] (map #(str (.-x %) " " (.-y %)) (.-points edge))
                  path (str "M " p1 " Q " p2 " " (str/join " T " prest))]
              [:g
                (for [p (.-points edge)]
                  [:circle {:x (.-x p) :y (.-y p) :radius 4 :fill "red"}])
                [:path {:key (str (.-v edge-id) "-" (.-w edge-id))
                        :stroke "black"
                        :fill "none"
                        :d path
                        :marker-end "url(#head)"}]]))]])))

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
    [:div.col.border.ml-3 {:style {:height 500}}
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
