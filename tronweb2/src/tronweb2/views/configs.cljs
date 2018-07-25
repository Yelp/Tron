(ns tronweb2.views.configs)

(defn view [state]
  (when-let [data (:api state)]
    (for [n (data "namespaces")]
      [:div.row {:key n}
       [:div.col [:a {:href (str "#/config/" n)} n]]])))
