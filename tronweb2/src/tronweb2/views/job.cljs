(ns tronweb2.views.job
  (:require [tronweb2.util :refer [format-time format-duration
                                   parse-in-local-time str->duration]]
            [reagent.core :as reagent]
            [cljs-time.core :as t]
            [cljs-time.coerce :as tc]
            [clojure.string :as str]
            [alanlcode.dagre]
            [cljsjs.d3]
            [timelines]))

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

(defn state->color [state]
  (case state
    "failed" "red"
    "succeeded" "green"
    "running" "magenta"
    "grey"))

(defn job-ag [ag ar]
  (let [g (js/dagre.graphlib.Graph.)
        runs (if ar
               (into {}
                     (map #(vector (% "action_name") %)
                          (ar "runs")))
               {})]
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
            (let [node (js->clj (.node g node-id))
                  run (or (runs (node "label")) {})]
              [:g {:key node-id}
                [:rect (merge node {:fill "white" :stroke (state->color (run "state"))
                                    :stroke-width 3
                                    "y" (- (node "y") (/ (node "height") 2))
                                    "x" (- (node "x") (/ (node "width") 2))})]
                [:a {:href (str "#/job/" (run "job_name") "/" (run "run_num") "/" (node "label"))}
                  [:text (merge node {:text-anchor "middle"
                                      :alignment-baseline "central"
                                      :style {:font-size "25px"}})
                         (node "label")]]]))
          (for [edge-id (.edges g)]
            (let [edge (.edge g edge-id)
                  [p1 p2 & prest] (map #(str (.-x %) " " (.-y %)) (.-points edge))
                  path (str "M " p1 " Q " p2 " " (str/join " T " prest))]
              [:path {:key (str (.-v edge-id) "-" (.-w edge-id))
                      :stroke "black"
                      :fill "none"
                      :d path
                      :marker-end "url(#head)"}]))]])))

(defn runs->d3tl [runs]
  (mapv
    #(let [start-time (parse-in-local-time (% "start_time"))]
      {"label" (% "run_num")
       "times" [{"starting_time"
                 (tc/to-long start-time)
                 "ending_time"
                 (tc/to-long
                  (if (% "duration")
                   (t/plus start-time (t/Period. 0 0 0 0 0 0 0 (str->duration (% "duration"))))
                   (t/now)))
                 "color" (state->color (% "state"))}]})
    (filter #(% "start_time") runs)))

(defn timeline [runs]
  (let [svg (reagent/atom nil) show 10]
    (fn [runs]
      (when @svg
        (-> (js/d3.select @svg)
            (.datum (clj->js (runs->d3tl (take show (sort-by #(% "start_date") runs)))))
            (.call (-> (.timelines js/d3)
                       (.tickFormat #js {:tickInterval 2 :tickSize 10})
                       (.stack)))))
      [:div.row.border.border-top-0.mb-3
        [:div.col.p-3
          [:svg {:width 896 :height (+ (* show 30) 50)
                 :ref #(if-not @svg (reset! svg %))}]]])))

(defn job [j ar]
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
     [job-ag (j "action_graph") ar]]]
   [:div.row.border.border-bottom-0
    [:div.col.p-3.mb-0.bg-dark.text-light.h5 "Timeline"]]
   [timeline (j "runs")]
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
    [job job-data (:job-actionrun state)]
    [:div.container "Loading..."]))
