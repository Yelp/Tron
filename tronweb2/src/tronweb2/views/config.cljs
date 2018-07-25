(ns tronweb2.views.config)

(defn view [state]
  (when-let [data (:config state)] [:pre [:code (data "config")]]))
