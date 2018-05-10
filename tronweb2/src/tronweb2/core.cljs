(ns tronweb2.core
  (:require [reagent.core :as reagent]
            [tronweb2.routes :as routes]))

(def app-state (reagent/atom {}))

(defn jobs [state]
  (for [j (:jobs state)]
    [:div.card.w-25 {:key (j "name")}
      [:div.card-body
        [:h6.card-title (str (j "name"))]
        [:p.card-text ""]]]))

(defn configs [state]
  (if-let [data (:api state)]
    (for [n (data "namespaces")]
      [:div.row {:key n}
        [:div.col-sm
          [:a {:href (str "#/config/" n)} n]]])))

(defn config [state]
  (if-let [data (:config state)]
    [:pre [:code (data "config")]]))

(defmulti render :view)
(defmethod render :jobs [state] (jobs state))
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
        [:h1 (:view-title state)]
        [:br]
        (render state)])])


(defn ^:export init []
  (enable-console-print!)
  (routes/setup app-state)
  (reagent/render [root] (.getElementById js/document "app")))
