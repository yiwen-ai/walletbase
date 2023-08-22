-- https://github.com/yiwen-ai/doc/blob/main/yiwen_coin.md
CREATE TABLE IF NOT EXISTS wallet (
    uid      BLOB,    -- user id, 12 bytes XID, https://docs.rs/xid/latest/xid/，系统账号为 ZERO
    sequence BIGINT,  -- transaction sequence, increase by 1 for each transaction
    award    BIGINT,  -- amount of Yiwen Coin awarded to the user, system account is negative
    topup    BIGINT,  -- amount of Yiwen Coin topuped to the user, system account is negative
    income   BIGINT,  -- amount of Yiwen Coin income to the user, system account is negative
    credits  BIGINT,  -- credits of the user, system account is 0
    txn      BLOB,    -- txn id that updates this wallet
    checksum BLOB,    -- HMAC 256/64 checksum of the wallet, HMAC(uid, sequence, award, topup, income, updated_by)
    PRIMARY KEY (uid)
) WITH caching = {'enabled': 'true'}
    AND comment = 'wallet'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE TABLE IF NOT EXISTS transaction (
    uid         BLOB,     -- user id
    id          BLOB,     -- transaction id
    sequence    BIGINT,   -- wallet 的 sequence
    payee       BLOB,     -- user id
    sub_payee   BLOB,     -- user id, sub payee
    status      TINYINT,  -- int8, -2: canceled, -1: canceling, 0: preparing, 1: prepared, 2: committing, 3: committed
    kind        TEXT,     -- award, topup, expenditure, sponsor, subscribe, etc.
    amount      BIGINT,   -- total amount of Yiwen Coin that payer pays
    sys_fee     BIGINT,   -- amount of Yiwen Coin to system as fee
    sub_shares  BIGINT,   -- amount of Yiwen Coin to sub payee
    description TEXT,     -- description
    payload     BLOB,     -- optional payload in CBOR format.
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'transactions'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX transaction_payee ON transaction (payee);
CREATE INDEX transaction_uid_status ON transaction ((uid), kind);

CREATE TABLE IF NOT EXISTS credit (
    uid         BLOB,    -- user id
    txn         BLOB,    -- txn id that initiates this credit log
    kind        TEXT,    -- kind of log: init, expenditure, income, award, etc.
    amount      BIGINT,  -- the amount of credits to add to the user
    description TEXT,    -- description
    PRIMARY KEY (uid, txn)
) WITH CLUSTERING ORDER BY (txn DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'credit logs'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

-- Stripe https://stripe.com/docs/api/charges/object
-- Ping++ https://www.pingxx.com/api/Charges%20%E6%94%AF%E4%BB%98%E6%A6%82%E8%BF%B0.html
CREATE TABLE IF NOT EXISTS topup (
    uid             BLOB,    -- user id
    id              BLOB,    -- topup order id
    status          TINYINT, -- int8, -2 failed, -1: refunded, 0: preparing, 1: prepared, 2: committing, 3: committed
    updated_at      BIGINT,  -- updated at, unix time, ms
    expire_at       BIGINT,  -- expire at, unix time, ms, default to created_at + 3600*1000
    quantity        BIGINT,  -- quantity of Yiwen Coin
    currency        TEXT,    -- three-letter ISO currency code, in lowercase. cny, usd, eur, cad, sgd, aud, gbp, hkd, jpy, krw
    amount          BIGINT,  -- amount intended to be collected by this payment. A positive integer representing how much to charge in the smallest currency unit (e.g., 100 cents to charge $1.00 or 100 to charge ¥100, a zero-decimal currency).
    amount_refunded BIGINT,  -- Amount in cents refunded (can be less than the amount attribute on the charge if a partial refund was issued).
    exchange        BIGINT,  -- 订单生成时刻锁定的汇率， 1000 Yiwen Coin = ? amount currency
    provider        TEXT,    -- 支付渠道，如 pingxx 为 Ping++，stripe 为 Stripe
    charge_id       TEXT,    -- 外部充值渠道的订单号，由充值渠道提供（如 Ping++ 的 charge id）
    charge_payload  BLOB,    -- CBOR 格式化的外部充值渠道的订单详情，由充值渠道返回
    txn             BLOB,    -- 充值成功时产生的 transaction id
    txn_refunded    BLOB,    -- 退款时产生的 transaction id
    failure_code    TEXT,    -- 订单的错误代码，由充值渠道提供（如 Ping++ 的 failure_code）
    failure_msg     TEXT,    -- 订单的错误消息的描述，由充值渠道提供（如 Ping++ 的 failure_msg）
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'topup orders'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX topup_uid_status ON topup ((uid), status);
