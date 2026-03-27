create table public.users
(
    user_id bigint not null
        primary key,
    phone   text
);

alter table public.users
    owner to airflow;

create table public.stores
(
    store_id bigint not null
        primary key,
    address  text,
    city     text
);

alter table public.stores
    owner to airflow;

create table public.drivers
(
    driver_id bigint not null
        primary key,
    phone     text
);

alter table public.drivers
    owner to airflow;

create table public.items
(
    item_id  bigint not null
        primary key,
    title    text,
    category text
);

alter table public.items
    owner to airflow;

create table public.orders
(
    order_id         bigint not null
        primary key,
    user_id          bigint
        references public.users,
    store_id         bigint
        references public.stores,
    created_at       timestamp,
    paid_at          timestamp,
    order_discount   numeric(5, 2),
    payment_type     text,
    delivery_cost    numeric(10, 2),
    delivery_address text
);

alter table public.orders
    owner to airflow;

create table public.order_status
(
    order_id            bigint not null
        primary key
        references public.orders,
    canceled_at         timestamp,
    cancellation_reason text
);

alter table public.order_status
    owner to airflow;

create table public.order_deliveries
(
    order_id            bigint
        references public.orders,
    driver_id           bigint
        references public.drivers,
    delivery_started_at timestamp,
    delivered_at        timestamp,
    delivery_id         bigserial
        primary key,
    constraint order_deliveries_pk
        unique (order_id, driver_id)
);

alter table public.order_deliveries
    owner to airflow;

create table public.order_items
(
    order_item_id     bigserial
        primary key,
    order_id          bigint
        references public.orders,
    item_id           bigint
        references public.items,
    quantity          integer,
    price             numeric(10, 2),
    discount          numeric(5, 2),
    canceled_quantity integer,
    replaced_item_id  bigint
        references public.items,
    constraint order_items_pk
        unique (order_id, item_id)
);

alter table public.order_items
    owner to airflow;

create table public.etl_state
(
    pipeline_name  text not null
        primary key,
    last_loaded_at timestamp
);

alter table public.etl_state
    owner to airflow;

create table public.users_staging
(
    user_id    bigint,
    phone      text,
    created_at timestamp
);

alter table public.users_staging
    owner to airflow;

create table public.stores_staging
(
    store_id   bigint,
    address    text,
    created_at timestamp
);

alter table public.stores_staging
    owner to airflow;

create table public.drivers_staging
(
    driver_id  bigint,
    phone      text,
    created_at timestamp
);

alter table public.drivers_staging
    owner to airflow;

create table public.items_staging
(
    item_id    bigint,
    title      text,
    category   text,
    created_at timestamp
);

alter table public.items_staging
    owner to airflow;

create table public.orders_staging
(
    order_id         bigint,
    user_id          bigint,
    store_id         bigint,
    created_at       timestamp,
    paid_at          timestamp,
    payment_type     text,
    order_discount   numeric(5, 2),
    delivery_cost    numeric(10, 2),
    delivery_address text
);

alter table public.orders_staging
    owner to airflow;

create table public.order_status_staging
(
    order_id            bigint,
    canceled_at         timestamp,
    cancellation_reason text,
    created_at          timestamp
);

alter table public.order_status_staging
    owner to airflow;

create table public.order_items_staging
(
    order_id          bigint,
    item_id           bigint,
    quantity          integer,
    price             numeric(10, 2),
    discount          numeric(5, 2),
    canceled_quantity integer,
    replaced_item_id  bigint,
    created_at        timestamp
);

alter table public.order_items_staging
    owner to airflow;

create table public.order_deliveries_staging
(
    order_id            bigint,
    driver_id           bigint,
    delivery_started_at timestamp,
    delivered_at        timestamp,
    created_at          timestamp
);

alter table public.order_deliveries_staging
    owner to airflow;

create table public.orders_mart
(
    dt                    date,
    turnover              numeric(32, 2),
    revenue               numeric(32, 2),
    created_orders        bigint not null,
    delivered_orders      bigint,
    canceled_orders       bigint,
    cancel_after_delivery bigint,
    service_errors        bigint,
    users                 bigint not null,
    avg_check             numeric(25, 6),
    active_drivers        bigint not null,
    profit                double precision
);

alter table public.orders_mart
    owner to airflow;

create table public.items_mart
(
    dt                 date,
    item_id            bigint,
    turnover           numeric(32, 2),
    ordered_qty        bigint,
    canceled_qty       bigint,
    orders_count       bigint not null,
    orders_with_cancel bigint not null
);

alter table public.items_mart
    owner to airflow;

