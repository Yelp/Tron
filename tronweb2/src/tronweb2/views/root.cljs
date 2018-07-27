(ns tronweb2.views.root
  (:require [tronweb2.views.job :as job]
            [tronweb2.views.jobrun :as jobrun]
            [tronweb2.views.jobs :as jobs]
            [tronweb2.views.actionrun :as actionrun]
            [tronweb2.views.configs :as configs]
            [tronweb2.views.config :as config]
            [tronweb2.views.schedule :as schedule]))

(defmulti render :view)
(defmethod render :jobs [state] (jobs/view state))
(defmethod render :job [state] (job/view state))
(defmethod render :jobrun [state] (jobrun/view state))
(defmethod render :actionrun [state] (actionrun/view state))
(defmethod render :configs [state] (configs/view state))
(defmethod render :config [state] (config/view state))
(defmethod render :schedule [state] (schedule/view state))

(defn view [state]
  [:div.container-fluid
   [:nav.navbar.navbar-expand-lg.navbar-light.bg-light
    [:a.navbar-brand {:href "#/jobs"} "Tron"]
    [:ul.navbar-nav.mr-auto
     [:li.nav-item [:a.nav-link {:href "#/jobs"} "Jobs"]]
     [:li.nav-item [:a.nav-link {:href "#/schedule"} "Schedule"]]
     [:li.nav-item [:a.nav-link {:href "#/configs"} "Configs"]]]
    [:form.form-inline.my-2.my-lg-0
     [:input.form-control.mr-sm-2 {:type "search" :placeholder "Search"}]
     [:button.btn.btn-outline-success.my-2.my-sm-0 {:type "submit"} "Search"]]]
   [:br]
   [:div.container
    [:h3 (:view-title state)]
    [:br]
    (render state)]])
