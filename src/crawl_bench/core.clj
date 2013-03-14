(ns crawl-bench.core
  (:require [itsy.core :as itsy]
            [cemerick.url :refer [url]]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.string :as str-tools])
  (:import fastPercentiles.PercentileRecorder
           fastPercentiles.Percentile
           [java.util TimerTask Timer])
  (:gen-class))

(def url-regex #"https?://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]")

(defn matching-extractor
  [filtering-regex]
  (fn [original-url body]
    (when body
      (let [candidates1 (->> (re-seq #"href=\"([^\"]+)\"" body)
                             (map second)
                             (remove #(or (= % "/")
                                          (.startsWith % "#")))
                             set)
            candidates2 (->> (re-seq #"href='([^']+)'" body)
                             (map second)
                             (remove #(or (= % "/")
                                          (.startsWith % "#")))
                             set)
            candidates3 (re-seq url-regex body)
            all-candidates (set (concat candidates1 candidates2 candidates3))
            matching-candidates (filter (partial re-matches filtering-regex) all-candidates)
            fq (set (filter #(.startsWith % "http") matching-candidates))
            ufq (set/difference (set matching-candidates) fq)
            fq-ufq (map #(str (url original-url %)) ufq)
            all (set (concat fq fq-ufq))]
        all))))

(defn blank-stats
  []
  (agent {
          :pages-total 0
          :runtime-percentile-rec (PercentileRecorder. 300000)
          :internal-percentile-rec (PercentileRecorder. 300000)
          :bytes-total 0
          :slow-urls (sorted-map-by (fn [a b] (compare b a)))
          }))


(def timer (java.util.Timer. "StatsPrinter" true))

(defn log-percentiles
  [title recorder]
  (let [percentiles (.percentiles recorder)]
    (log/info
     (format "%s: 10th: %d, 50th: %d, 95th: %d, 99th: %d"
             title
             (.median (aget percentiles 10))
             (.median (aget percentiles 50))
             (.median (aget percentiles 95))
             (.median (aget percentiles 99))))))

(defn log-stats-agg
  [{:keys [runtime-percentile-rec internal-percentile-rec
           slow-urls pages-total bytes-total]}]
  (log/info
   (format "Crawled: %d, Bytes Total: %d",
    pages-total
    bytes-total))
  (log-percentiles "Total runtime" runtime-percentile-rec)
  (log-percentiles "Header runtime" internal-percentile-rec))

(defn log-stats-slowest
  [{slow-urls :slow-urls}]
  (log/info "10 Slowest URLs (By x-rack-cache)")
  (doseq [line (map
                (fn [[time url]]
                  (format "%dms %s" time url))
                (reduce (fn [acc [t urls]]
                          (concat acc
                                  (map (fn [u] [t u]) urls)))
                        []
                        (take 20 slow-urls)))]
    (log/info line)))

(defn periodic-stats-printer
  [stats-agent]
  (.schedule
   timer
   (proxy [TimerTask] []
     (run []
       (let [stats @stats-agent]
         (log/info "---")
         (log-stats-agg stats)
         (log-stats-slowest stats))))
   5000 5000))


(defn add-slow-url
  [{slow-urls :slow-urls :as stats} url runtime]
  (assoc stats :slow-urls
         (assoc slow-urls runtime
                (conj (slow-urls runtime []) url))))

(defn sec-str-to-millis
  [s]
  (int (* 1000 (Float/parseFloat s))))

(defn gen-handler
  [page-stats]
  (fn [{:keys [runtime url body headers] :as resp}]
    (log/debug (format "%s bytes in %sms for %s"
                       runtime url (.length body)))
    (send page-stats
          (fn [{:keys [bytes-total slow-urls] :as stats}]
            (let [xruntime (sec-str-to-millis (headers "x-runtime"))]
              (.record (:runtime-percentile-rec stats) runtime)
              (.record (:internal-percentile-rec stats)
                       (if (= (headers "x-rack-cache") "fresh")
                         0
                         xruntime))
              (-> stats
                  (update-in [:pages-total] inc)
                  (update-in [:bytes-total] (partial + (.length body)))
                  (add-slow-url url xruntime)))))))

(defn run
  [workers url pattern]
  (let [stats (blank-stats)]
    (periodic-stats-printer stats)
    (itsy/crawl
     {:url url
      :workers workers
      :host-limit true
      :url-limit -1
      :url-extractor (matching-extractor pattern)
      :handler (gen-handler stats)})))

(defn -main [workers url pattern & rest]
  (run (Integer/parseInt workers) url (re-pattern pattern)))
