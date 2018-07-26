(ns tronweb2.util
  (:require [clojure.contrib.humanize :as humanize]
            [cljs-time.format :as tf]
            [cljs-time.core :as t]
            [clojure.string :as str]))

(defn str->duration [s]
  (let [[h m s] (str/split s #":")]
    (* 1000
       (+ (* 3600 (js/parseFloat h))
          (* 60 (js/parseFloat m))
          (js/parseFloat s)))))

(defonce time-formatter (tf/formatter "YYYY-MM-dd HH:mm:ss"))
(defn parse-in-local-time [t]
  (let [offset (.getTimezoneOffset (js/Date.))]
    (t/plus (tf/parse time-formatter t)
            (t/Period. 0 0 0 0 0 offset (* 8 3600) 0))))

(defn format-time [t]
  (if (and t (not= "" t))
    [:abbr {:title t} (humanize/datetime (parse-in-local-time t))]
    "-"))

(defn format-duration [d]
  (if (and d (not= "" d))
    [:abbr {:title d} (humanize/duration (str->duration d) {:number-format str})]
    "-"))
