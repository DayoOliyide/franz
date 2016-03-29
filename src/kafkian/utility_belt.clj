(ns ^{:doc "Collection of useful tools"}
    kafkian.utility-belt
  )



(defn- extract-map-keys [m-data map-pattern]
  (loop [pattern map-pattern
         result m-data]
    (if (empty? pattern)
      result
      (let [[k v] (first pattern)
            comp-key {k v}]
        (cond
          (= :ONLY v) (reduce #(apply conj %1 (vals %2)) [] (distinct (if (sequential? result)
                                                                        (mapcat keys result)
                                                                        (keys result))))

          (= :ANY v) (recur (rest pattern) (if (sequential? result)
                                             (mapcat vals result)
                                             (vals result)))

          :else (recur (rest pattern) (if (sequential? result)
                                        (mapcat #(get % comp-key) result)
                                        (get result comp-key))))))))

(defn- match-key-map
  ""
  [key-map map-set]
  (let [[k v] (first key-map)
        filter-fn (if (= :ANY v)
                    #(= k (key (first %)))
                    #(= key-map %))]
    (seq (filter filter-fn map-set))))

(defn- match-map-pattern
  ""
  [m-data map-pattern]
  (let [level-keys [{:group :ANY} {:name :ANY} {:tags :ANY}]
        match-keys (mapv #(apply hash-map %) map-pattern)]
    (clojure.walk/postwalk (fn [x]
                             (if (and (map? x) (every? map? (keys x)))
                               (let [keys-set (into #{} (keys x))
                                     matched-levels (mapcat #(match-key-map % keys-set) level-keys)
                                     matched-keys (mapcat #(match-key-map % keys-set) match-keys)
                                     matched-levels? (seq matched-levels)
                                     matched-keys? (seq matched-keys)]
                                 (if (or matched-levels? matched-keys?)
                                   (if matched-keys?
                                     (let [k-with-vals (filter #(seq (get x %)) matched-keys)]
                                       (apply dissoc x (apply disj keys-set k-with-vals)))
                                     {})
                                   x))
                               x))
                           m-data)))

(defn metrics-lens
  "Takes a metrics map and selects parts of it depending on the given options.
    NOTE
        1) It navigates in the order :group :name :tags and returns a map matching
           in order
        2) The :ANY value matches key for the given key. (Equivalent to regex *)
        3) The :ONLY value indicates that navigation stops there and only the distinct
           key elements at that level are returned.

  USAGE:
  ; Returns a map containing only metric group \"group-a\"
  (metrics-lens metrics-map :group \"group-a\")

  ; Returns a map containing only metric group \"group-a\" containing only metric name \"name-a\"
  (metrics-lens metrics-map :group \"group-a\" :name \"name-a\")

  ; Returns a map containing only metric group \"group-a\" containing only metric name \"name-a\"
  ; containing only tags {\"tag-a\" \"tag-a-value\" \"tag-b\" \"tag-b-value\"}
  (metrics-lens metrics-map :group \"group-a\" :name \"name-a\" :tags {\"tag-a\" \"tag-a-value\" \"tag-b\" \"tag-b-value\"})

  ; Returns a map containing metric groups containing only metric names \"name-a\"
  (metrics-lens metrics-map :group :ANY :name \"name-a\")
  (metrics-lens metrics-map :group :ANY :name \"name-a\" :tags :ANY)

  ; Returns a map containing metric groups containing metrics names with only
  ; tag {\"tag-a\" \"tag-a-value\"}
  (metrics-lens metrics-map :group :ANY :name :ANY :tags {\"tag-a\" \"tag-a-value\"})

  ; Returns a vector of distinct metric group names
  (metrics-lens metrics-map :group :ONLY)

  ; Returns a vector of distinct metric names
  (metrics-lens metrics-map :name :ONLY)

  ; Returns a vector of distinct metric tags
  (metrics-lens metrics-map :tags :ONLY)

  ; Returns a vector of distinct metric names found within metric group \"group-a\"
  (metrics-lens metrics-map :group \"group-a\" :name :ONLY)

  ; Returns a vector of distinct metric tags found within metric group \"group-a\"
  ; and metric name \"name-a\"
  (metrics-lens metrics-map :group \"group-a\" :name \"name-a\" :tags :ONLY)

  ; The first ONLY it encounters is where the lens stops
  ; The below will only return all the metric names for the metric group \"group-a\",
  (metrics-lens metrics-map :group \"group-a\" :name :ONLY :tags {\"tag-a\" \"tag-a-value\" \"tag-b\" \"tag-b-value\"})

  "
  [m-data & {:keys [group name tags]
             :or {group :ANY
                  name :ANY
                  tags :ANY}}]
  (let [lens-args {:group group :name name :tags tags}];; ensure entries are in the righ order
    (if (some #(= :ONLY %) (vals lens-args))
      (extract-map-keys m-data lens-args)
      (match-map-pattern m-data lens-args))))


