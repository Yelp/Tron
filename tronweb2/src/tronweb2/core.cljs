(ns tronweb2.core
  (:require [reagent.core :as reagent]
            [tronweb2.routes :as routes]
            [clojure.contrib.humanize :as humanize]
            [cljs-time.format :as tf]
            [cljs-time.coerce :as tc]
            [cljs-time.core :as t]
            [clojure.string :as str]))

(defonce app-state (reagent/atom {}))

(defn log [& args]
  (.log js/console args))

(defn duration-from-str [s]
  (let [[h m s] (str/split s #":")
        s (str/replace s #"^0+" "0")]
    (* 1000
       (+ (* 3600 (js/parseFloat h))
          (* 60 (js/parseFloat m))
          (js/parseFloat s)))))

(defn jobs [state]
  [:div.row
    (for [{job-name "name"} (:jobs state)]
      [:div.col-sm-3.mb-4 {:key job-name}
        [:div.card
          [:div.card-body
            [:h6.card-title [:a {:href (str "#/job/" job-name)} (str job-name)]]
            [:p.card-text ""]]]])])

(defn configs [state]
  (when-let [data (:api state)]
    (for [n (data "namespaces")]
      [:div.row {:key n}
        [:div.col [:a {:href (str "#/config/" n)} n]]])))

(defn config [state]
  (when-let [data (:config state)] [:pre [:code (data "config")]]))

(defn parse-in-local-time [t]
  (let [offset (.getTimezoneOffset (js/Date.))]
    (tf/parse (tf/formatter "YYYY-MM-dd HH:mm:ss") t)))
    ; (t/plus (tf/parse (tf/formatter "YYYY-MM-dd HH:mm:ss") t))))
    ;         (t/Period. 0 0 0 0 0 offset (* 8 3600) 0))))

(defn format-time [t]
  (if (and t (not= "" t))
    [:abbr {:title t} (humanize/datetime (parse-in-local-time t))]
    "-"))

(defn format-duration [d]
  (if (and d (not= "" d))
    [:abbr {:title d} (humanize/duration (duration-from-str d) {:number-format str})]
    "-"))

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
            ms (duration-from-str dr)
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

(defn job- [j]
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

(defn job [state]
  (if-let [job (:job state)]
    (job- job)
    [:div.container "Loading..."]))

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

(defn jobrun [state]
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
          (job-ag (jr "action_graph"))]]
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

(defn actionrun- [ar history]
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

(defn actionrun [state]
  (let [jr (or (:jobrun state) {})
        runs (or (jr "runs") [])
        an (:actionrun-name state)
        ar (first (filter #(= an (% "action_name")) runs))
        ah (or (:actionrun-history state) [])]
    (if ar
      (actionrun- ar ah)
      [:div.container "Loading..."])))

(defmulti render :view)
(defmethod render :jobs [state] (jobs state))
(defmethod render :job [state] (job state))
(defmethod render :jobrun [state] (jobrun state))
(defmethod render :actionrun [state] (actionrun state))
(defmethod render :configs [state] (configs state))
(defmethod render :config [state] (config state))

(defn root []
  [:div.container-fluid
    [:nav.navbar.navbar-expand-lg.navbar-light.bg-light
      [:a.navbar-brand {:href "#/"} "Tron"]
      [:ul.navbar-nav.mr-auto
        [:li.nav-item
          [:a.nav-link {:href "#/"} "Jobs"]]
        [:li.nav-item
          [:a.nav-link {:href "#/configs"} "Configs"]]]
      [:form.form-inline.my-2.my-lg-0
        [:input.form-control.mr-sm-2 {:type "search" :placeholder "Search"}]
        [:button.btn.btn-outline-success.my-2.my-sm-0 {:type "submit"} "Search"]]]
    [:br]
    (let [state @app-state]
      [:div.container
        [:h3 (:view-title state)]
        [:br]
        (render state)])])

(defn ^:export init []
  (enable-console-print!)
  (routes/setup app-state)
  (reagent/render [root] (.getElementById js/document "app")))

(defn ^:export reload []
  (reagent/render [root] (.getElementById js/document "app"))
  (reagent/force-update-all))
