(ns exstate.core
  (:require [clojure.set :as set]))


;;; Protocols

(defprotocol IService
  "A non-deterministic external state provider"
  (-observe [this path])
  (-notify [this path message]))

(defprotocol IDatabase
  "A collection of services"
  (-snapshot! [this]))

(defprotocol ISnapshot
  "A memoization of observations about a database at a point in time"
  ;;TODO some kind of version number for comparisons?
  (-observation [this endpoint]))

(defprotocol IMapping
  "Assigns services to paths via a hierarchical routing table"
  (-mount [this endpoint path mode])
  (-bind [this src dest mode])
  (-unmap [this path])
  (-endpoints [this path]))

(defprotocol ITree
  "A snapshot of a database viewed with a particular hierarchical mapping"
  (-mapping [this])
  (-deref-at [this path]))


;;; Types

(deftype Tree [mapping snapshot]
  ITree
  (-deref-at [this path]
    (->> (-endpoints mapping path)
         (map #(-observation snapshot %))
         (remove nil?)
         first)))

(defn- node-path [path]
  (interleave (repeat :children) path))

(defn- endpoint-path [path ord]
  (concat (node-path path) [:endpoints ord]))

(deftype Mapping [database min-ord max-ord routes]

  clojure.lang.IDeref
  (deref [this]
    (Tree. this @database))

  IMapping

  (-mount [this endpoint path mode]
    (case mode
      :before
        (let [ord (dec min-ord)
              ks (endpoint-path path ord)
              routes* (assoc-in routes ks endpoint)]
          (Mapping. database ord max-ord routes*))
      :after
        (let [ord (inc max-ord)
              ks (endpoint-path path ord)
              routes* (assoc-in routes ks endpoint)]
          (Mapping. database min-ord ord routes*))
      :replace
        (let [ks (endpoint-path path 0)
              routes* (assoc-in routes ks endpoint)]
          (Mapping. database min-ord max-ord routes*))))

  (-bind [this src dest mode]
    ;;TODO this requires some trickery after via -endpoints
    )

  (-unmap [this path]
    ;;TODO remove subtree worth of routes
    )

  (-endpoints [this path]
    (->> path
         (reductions (fn [subroutes component]
                       (get-in subroutes [:children component]))
                     routes)
         (map :endpoints)
         (map vector (iterate next path))
         (reduce (fn [matches [relpath endpoints]]
                   (let [endpoints* (for [[ord [service root]] endpoints]
                                      [ord [service (into root relpath)]])]
                     (if (get endpoints 0)
                       (apply sorted-set endpoints*)
                       (into matches endpoints*))))
                 (sorted-set))
         (map second)))

  )

(deftype Snapshot [database data]
  ISnapshot
  (-observation [this [service path]]
    (dosync
      (let [p (cons service (node-path path))
            node (get-in @data p)]
        (cond
          (and node (contains? node :value))
            (:value node)
          (= @database this)
            (let [x (-observe service path)]
              (alter data update-in p assoc :value x)
              x))))))

(deftype Database [services snapshot]

  IDatabase
  (-snapshot! [this]
    (dosync
      (let [new-snapshot (Snapshot. this (ref {}))]
        (ref-set snapshot new-snapshot))))

  clojure.lang.IDeref
  (deref [this]
    (dosync
      (or @snapshot (-snapshot! this))))

  )


;;; Public interface

(defn create-database []
  (Database. (ref {}) (ref nil)))

(defn snapshot! [database]
  (-snapshot! database))

(defn empty-mapping [database]
  (Mapping. database 0 0 {}))

(def mapping-modes #{:before :after :replace})

(defn mount
  ([mapping endpoint path]
   (mount mapping endpoint path :replace))
  ([mapping endpoint path mode]
   {:pre [(vector? path)
          (mapping-modes mode)]}
   (let [endpoint* (if (satisfies? IService endpoint)
                     [endpoint []]
                     endpoint)]
     (-mount mapping endpoint* path mode))))

(defn bind [mapping src dest mode]
  {:pre [(vector? src)
         (vector? dest)
         (mapping-modes mode)]}
  (-bind mapping src dest mode))

(defn unmap [mapping path]
  {:pre [(vector? path)]}
  (-unmap mapping path))

(defn deref-at [tree path]
  {:pre [(vector? path)]}
  (-deref-at tree path))


;;; Some Services

(deftype CounterService [counters]
  IService
  (-notify [this path message]
    ;;TODO validate args
    ;;TODO is it OK to do this synchronously? I think so...
    ;;TODO is the ::value trick acceptable?
    (assert (vector? path))
    (let [[cmd & args] message
          path (conj path ::value)]
      (case cmd
        :inc (swap! counters update-in path (fnil inc 0))
        :add (swap! counters update-in path (fnil + (first args))))))
  (-observe [this path]
    (assert (vector? path))
    (get-in @counters path 0)))

(defmacro readonly-service [args & body]
  `(reify
     IService
     (-observe [this# ~@args]
       ~@body)
     (-notify [this# path# message#]
       nil)))

(def clock-service
  (readonly-service [_]
    (System/currentTimeMillis)))

(defn constant-tree [x]
  (readonly-service [_]
    x))

(defn constant-root [x]
  (readonly-service [path]
    (when (= path [])
      x)))

(comment

  (def counter-service (CounterService. (atom {})))

  (def db (create-database))
  (deref db)

  (snapshot! db)

  ;;;

  (def mnt (-> (empty-mapping db)
               (mount clock-service [:clock])
               (mount counter-service [:counters])))

  (fipp.edn/pprint (.routes mnt))

  (def t @mnt)

  (deref-at t [:clock])
  (deref-at t [:clock :foo])

  (snapshot! db)
  (def t* @mnt)

  (deref-at t* [:clock])
  (deref-at t* [:clock :foo])

  ;;;

  (def mnt (-> (empty-mapping db)
               (mount (constant-tree :a) [])
               (mount (constant-tree :b) [:foo])
               (mount (constant-root :c) [:foo :bar] :before)
               (mount (constant-root :d) [:foo :baz] :after)
               ))

  (fipp.edn/pprint (.routes mnt))

  (-endpoints mnt [])
  (-endpoints mnt [:foo])
  (-endpoints mnt [:foo :bar])
  (-endpoints mnt [:foo :baz])
  (-endpoints mnt [:foo :baz :bat])

  (def t @mnt)

  (deref-at t [])
  (deref-at t [:foo])
  (deref-at t [:foo :x])
  (deref-at t [:foo :bar])
  (deref-at t [:foo :bar :y])
  (deref-at t [:foo :baz])
  (deref-at t [:foo :baz :z])

)
