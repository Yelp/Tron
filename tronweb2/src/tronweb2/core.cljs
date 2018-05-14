(ns tronweb2.core
  (:require [reagent.core :as reagent]
            [tronweb2.routes :as routes]
            [clojure.contrib.humanize :as humanize]
            [cljs-time.format :as tf]
            [cljs-time.coerce :as tc]
            [cljs-time.core :as t]
            [clojure.string :as str]))

(defn duration-from-str [s]
  (let [[h m s] (str/split s #":")]
    (+ (int h) (int m) (float s))))

(def app-state (reagent/atom {}))

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

(defn format-time [t]
  (if (and t (not= "" t))
    [:abbr {:title t}
      (humanize/datetime
        (tf/parse (tf/formatter "YYYY-MM-dd HH:mm:ss") t))]
    "-"))

(defn format-duration [d]
  (if (and d (not= "" d))
    [:abbr {:title d} (humanize/duration (duration-from-str d))]
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
    [:div.col.small (format-duration (jr "duration"))]])

(defn job- [j]
  [:div.container
    [:div.row.mb-3
      [:div.col.border.p-3.mr-3
        [:h5 "Details"]
        [:div.row [:div.col "Status"] [:div.col (j "status")]]
        [:div.row [:div.col "Node pool"]
                  [:div.col (get-in j ["node_pool" "name"])]]
        [:div.row [:div.col "Schedule"] [:div.col (j "schedule")]]
        [:div.row [:div.col "Settings"] [:div.col "-"]]
        [:div.row [:div.col "Last run"] [:div.col (format-time (j "last_run"))]]
        [:div.row [:div.col "Next run"] [:div.col (format-time (j "next_run"))]]]
      [:div.col.border.p-3.ml-3
        [:h5 "Action graph"]]]
    [:div.row.mb-3.border
      [:div.col.p-3.mb-0.bg-dark.text-light.h5 "Timeline"]]
    [:div.row.border.border-bottom-0
      [:div.col.p-3.mb-0.bg-dark.text-light.h5 "Job runs"]]
    [:div.row.border.border-top-0
      [:div.col.p-3
        [:div.row.mb-1
          [:div.col.h6 "Id"]
          [:div.col.h6 "State"]
          [:div.col.h6 "Node"]
          [:div.col.h6 "Run time"]
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
          [:h5.row.p-3.bg-dark.text-light "Action graph"]]]
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