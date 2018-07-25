(ns tronweb2.views.jobs)

(defn view [state]
  [:div.row
   (for [{job-name "name"} (:jobs state)]
     [:div.col-sm-3.mb-4 {:key job-name}
      [:div.card
       [:div.card-body
        [:h6.card-title [:a {:href (str "#/job/" job-name)} (str job-name)]]
        [:p.card-text ""]]]])])
