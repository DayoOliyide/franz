(ns ^{:doc "Collection of useful tools"} kafkian.utility-belt
  (:require [com.rpl.specter :as spec]))

(def METRIC-GROUP-PATH [spec/ALL spec/FIRST :group])
(def METRIC-NAME-PATH [spec/ALL (spec/view second) spec/ALL spec/FIRST :name])
(def METRIC-TAGS-PATH [spec/ALL (spec/view second) spec/ALL (spec/view second) spec/ALL spec/FIRST :tags])

(def METRIC-NAME-PARTIAL [spec/ALL spec/FIRST :name])
(def METRIC-TAGS-PARTIAL [spec/ALL spec/FIRST :tags])


(defn metrics-selector
  "Takes a map with :group :name & :tags keys and generates the appropriate spector selector
   to navigate a metrics map.
   NOTE
       1) The selector is created to navigate in the order :group :name :tags
       2) If the value :ONLY is encountered, the selector will not include any subsequent
          keys/values and will select only the key elements at the level it encountered
          :ONLY"
  [m]
  (let [lens-args (select-keys m [:group :name :tags]) ;; ensure entires are in right order
        split (split-with #(not= :ONLY (val %)) lens-args)
        up-to-only (into {} (first split))
        from-only (into {} (second split))
        lens-args (if (= lens-args up-to-only)
                    lens-args ;; there was no :ONLY in any map-entry
                    (conj up-to-only (first from-only))) ;; keep up to the first :ONLY map-entry
        translate-fn (fn [[k v]]
                       (cond
                         (= :ONLY v) (condp = k
                                       :group METRIC-GROUP-PATH
                                       :name METRIC-NAME-PARTIAL
                                       :tags METRIC-TAGS-PARTIAL)
                         (some? v) (condp = k
                                     :group [(spec/keypath {:group v})]
                                     :name [(spec/keypath {:name v})]
                                     :tags [(spec/keypath {:tags v})])
                         :else nil))
        selector (vec (mapcat translate-fn lens-args))]
    (cond
      (= selector METRIC-NAME-PARTIAL) METRIC-NAME-PATH
      (= selector METRIC-TAGS-PARTIAL) METRIC-TAGS-PATH
      :else selector)))


(defn metrics-lens
  "Takes a metrics map and selects parts of it depending on the given options.
    NOTE
        1) It navigates in the order :group :name :tags
        2) The :ONLY value indicates that navigation stops there and only the distinct
           key elements at that level are returned.

  USAGE:

  (metrics-lens metrics-map :group \"Group-A\")
  (metrics-lens metrics-map :group \"Group-A\" :name \"Group-A-NameA\")
  (metrics-lens metrics-map :group \"Group-A\" :name \"Group-A-NameA\" :tags {\"TagAA-1\" \"TagAA-1-value\" \"TagAA-2\" \"TagAA-2-value\"})
  (metrics-lens metrics-map :group :ONLY)
  (metrics-lens metrics-map :name :ONLY)
  (metrics-lens metrics-map :tags :ONLY)
  (metrics-lens metrics-map :group \"Group-A\" :name :ONLY)
  (metrics-lens metrics-map :group \"Group-B\" :name :ONLY)
  (metrics-lens metrics-map :group \"Group-A\" :name \"Group-A-NameA\" :tags :ONLY)
  (metrics-lens metrics-map :group \"Group-B\" :name \"Group-B-NameA\" :tags :ONLY)
  ;;The first ONLY it encounters is where the lens stops
  ;;The below will only return all the metric names for the metric group \"Group-A\",
  (metrics-lens metrics-map :group \"Group-A\" :name :ONLY :tags {\"TagAA-1\" \"TagAA-1-value\" \"TagAA-2\" \"TagAA-2-value\"})

  "
  [met-map & {:keys [group name tags] :as lens-args}]
  (if (or group name tags)
    (if (some #(= :ONLY (val %)) lens-args)
      (distinct (spec/select (metrics-selector lens-args) met-map))
      (spec/select-first (metrics-selector lens-args) met-map))
    met-map))
