(ns crawl-bench.core
  (:require [itsy.core :as itsy]
            [clojure.tools.logging :as log]
            [clojure.string :as str-tools])
  (:import fastPercentiles.PercentileRecorder
           fastPercentiles.Percentile
           [java.util TimerTask Timer])
  (:gen-class))

(defn blank-stats
  []
  (agent {
          :percentiles (PercentileRecorder. 300000)
          :bytes-total 0
          :slow-urls (sorted-map-by (fn [a b] (compare b a)))
          }))


(def timer (java.util.Timer. "StatsPrinter" true))

(defn periodic-stats-printer
  [stats-agent]
  (.schedule
   timer
   (proxy [TimerTask] []
     (run []
       (let [stats @stats-agent
             percentiles (.percentiles (:percentiles stats))]
         (log/info (format "Bytes Total: %d, 10th: %d, 50th: %d, 95th: %d, 99th: %d"
                           (:bytes-total stats)
                           (.median (aget percentiles 10))
                           (.median (aget percentiles 50))
                           (.median (aget percentiles 95))
                           (.median (aget percentiles 99))))
         (log/info (format "10 Slowest URL Timings"))
         (doseq [line (map
                        (fn [[time urls]]
                          (format "%dms %s"
                                  time
                                  (str-tools/join ", " urls)))
                        (take 10 (:slow-urls stats)))]
           (log/info line))
         )))
   1000 1000))

(defn gen-handler
  [page-stats]
  (fn [{:keys [runtime url body] :as resp}]
    (log/debug (format "%s bytes in %sms for %s" runtime url (.length body)))

    (send page-stats
          (fn [{:keys [bytes-total slow-urls] :as stats}]
            (let [slow-rt (conj (slow-urls runtime []) url)
                  slow (assoc slow-urls runtime slow-rt)]
              (.record (:percentiles stats) runtime)
              (-> stats
                  (assoc :bytes-total (+ bytes-total (.length body)))
                  (assoc :slow-urls slow)))))))

(defn run
  [workers url]
  (let [stats (blank-stats)]
    (periodic-stats-printer stats)
    (itsy/crawl
     {:url url
      :workers workers
      :host-limit true
      :handler (gen-handler stats)})))

(defn -main [workers url & rest]
  (run (Integer/parseInt workers) url))
