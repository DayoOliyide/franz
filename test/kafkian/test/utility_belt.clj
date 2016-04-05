(ns kafkian.test.utility-belt
  (:use [midje.sweet]
        [kafkian.utility-belt]))


(facts "About metrics-lens function"

       (def m-data {{:group "group-a"} {{:name "name-a"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-a" :value 1}}}
                          {:group "group-b"} {{:name "name-a"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-a" :value 2}}
                                              {:name "name-b"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-b" :value 3}
                                                                {:tags {"tag-b" "tag-b-value"}} {:description "metric name-b" :value 4}}}
                          {:group "group-c"} {{:name "name-c"} {{:tags {"tag-c" "tag-c-value"}} {:description "metric name-c" :value 5}}}})

       (fact ":ONLY returns a distinct vector of key 'names' "
             (metrics-lens m-data :group :ONLY) => ["group-a" "group-b" "group-c"]
             (metrics-lens m-data :name :ONLY) => ["name-a" "name-b" "name-c"]
             (metrics-lens m-data :tags :ONLY) => [{"tag-a" "tag-a-value"}
                                                   {"tag-b" "tag-b-value"}
                                                   {"tag-c" "tag-c-value"}])

       (fact ":ONLY, if preceded by a key pattern,
              returns a distinct vector of key 'names' under that key pattern"
             (metrics-lens m-data :group "group-a" :name :ONLY) => ["name-a"]
             (metrics-lens m-data :group "group-b" :name :ONLY) => ["name-a" "name-b"]
             (metrics-lens m-data :group "group-c" :name :ONLY) => ["name-c"]

             (metrics-lens m-data :group "group-b" :name "name-b" :tags :ONLY) => [{"tag-a" "tag-a-value"}
                                                                                   {"tag-b" "tag-b-value"}]
             )

       (fact ":ONLY, the matching stops at the first :ONLY encountered"
             (metrics-lens m-data :group :ONLY :name :ONLY :tags :ONLY)
             => (metrics-lens m-data :group :ONLY)

             (metrics-lens m-data :group "group-a" :name :ONLY :tags :ONLY)
             => (metrics-lens m-data :group "group-a" :name :ONLY))

       (fact ":ANY, missing keys are automatically created with :ANY"
             (metrics-lens m-data) => (metrics-lens m-data :group :ANY :name :ANY :tags :ANY)
             (metrics-lens m-data :tags :ANY) => (metrics-lens m-data :group :ANY :name :ANY :tags :ANY)
             (metrics-lens m-data :name "name-c" :tags :ANY)
             => (metrics-lens m-data :group :ANY :name "name-c" :tags :ANY)
             )

       (fact ":ANY, match all key patterns for that key"
             (metrics-lens m-data :group :ANY) => m-data
             (metrics-lens m-data :group :ANY :name :ANY :tags {"tag-a" "tag-a-value"})
             => {{:group "group-a"} {{:name "name-a"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-a"
                                                                                        :value 1}}}
                 {:group "group-b"} {{:name "name-a"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-a"
                                                                                        :value 2}}
                                     {:name "name-b"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-b"
                                                                                        :value 3}}}}

             (metrics-lens m-data :group :ANY :name "name-c" :tags :ANY)
             => {{:group "group-c"} {{:name "name-c"} {{:tags {"tag-c" "tag-c-value"}} {:description "metric name-c"
                                                                                        :value 5}}}}

             (metrics-lens m-data :group "group-b" :name "name-b" :tags :ANY)
             => {{:group "group-b"} {{:name "name-b"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-b"
                                                                                        :value 3}
                                                       {:tags {"tag-b" "tag-b-value"}} {:description "metric name-b"
                                                                                        :value 4}}}}
             )

       (fact "match specific key patterns"
             (metrics-lens m-data :group "group-b" :name "name-a" :tags {"tag-a" "tag-a-value"})
             => {{:group "group-b"} {{:name "name-a"} {{:tags {"tag-a" "tag-a-value"}} {:description "metric name-a"
                                                                                        :value 2}}}}

             (metrics-lens m-data :group "group-c" :name "name-c" :tags {"tag-c" "tag-c-value"})
             => {{:group "group-c"} {{:name "name-c"} {{:tags {"tag-c" "tag-c-value"}} {:description "metric name-c"
                                                                                       :value 5}}}})

       (fact ":ONLY for unmatched patterns return an empty vector"
             (metrics-lens m-data :group "IdontEXIST" :name :ONLY) => [])

       (fact "Specific key patterns that don't match anything return an empty map"
             (metrics-lens m-data :group "IdontEXIST" :name "name-b" :tags {"tag-a" "tag-a-value"})
             => {}

             (metrics-lens m-data :group "group-b" :name "name-a" :tags {"zero" "xEfWfe"})
             => {})

       )
