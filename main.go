package main

import (
    "strconv"
    "encoding/json"
    "time"
    "fmt"
	. "github.com/jeffdoubleyou/EthereumAPI"
    "github.com/go-redis/redis"
    "github.com/spf13/viper"
    "go.uber.org/zap"
    "github.com/shopspring/decimal"
    "./struct"
)

var log *zap.SugaredLogger
var RedisClient *redis.Client

func main() {

    viper.SetConfigName("config")
    viper.AddConfigPath(".")
    viper.SetDefault("redis.host", "127.0.0.1")
    viper.SetDefault("redis.port", "6379")
    viper.SetDefault("redis.password", "")
    viper.SetDefault("node.host", "127.0.0.1")
    viper.SetDefault("node.port", "8545")
    viper.SetDefault("limits.recentBlocks", 10)
    viper.SetDefault("limits.recentTransactions", 25)
    viper.SetDefault("log.path", "./blockwatcher.log")

    err := viper.ReadInConfig();
    if(err != nil) {
        panic(fmt.Errorf("Unable to open config : %s\n", err))
    }

    logger, _ := zap.NewProduction()
    defer logger.Sync()
    log = logger.Sugar()
    log.Info("Starting up")

    redisClient := redis.NewClient(&redis.Options{
        Addr:   viper.GetString("redis.host")+":"+viper.GetString("redis.port"),
        Password: viper.GetString("redis.password"),
        DB: 0,
    })

    RedisClient = redisClient

    SetServer(viper.GetString("node.host")+":"+viper.GetString("node.port"))

    currentBlock := getCurrentBlockNumber()
    lastBlock := getLastBlock(redisClient)

    fmt.Println("Current block number:", currentBlock)
    fmt.Println("Last block imported:", lastBlock)

    lastBlock++;

    run := 1

    for run == 1 {
        for lastBlock <= currentBlock {
            log.Infof("Going to get block #%d", lastBlock)
            res,err  := EthGetBlockByNumber(strconv.FormatInt(lastBlock, 10), true)

            if err != nil {
                log.Errorf("%s\n", err)
                run = 0
            }

            recent, err := populateRecent(redisClient, res)

            if(err != nil) {
                log.Errorf("Failed to update recent blocks: %s", err);
            } else {
                log.Debugf("Updated recent blocks: %s", recent)
            }

            recent, err = populateTransactions(redisClient, res)

            if(err != nil) {
                log.Errorf("Failed to update recent transactions: %s", err);
            } else {
                log.Debugf("Updated recent transactions: %s", recent)
            }

            uncles, _ := populateUncles(res)

            log.Debugf("Inserted %d uncles", uncles)

            miner, _ := populateMiner(res)

            if(miner) {
                log.Debugf("Updated miner info")
            }

            lastBlock, err = setLastBlock(redisClient, res)
            lastBlock++
        }
        log.Debugf("All cought up, waiting for a couple seconds");
        time.Sleep(2000*time.Millisecond)
        currentBlock = getCurrentBlockNumber()
        log.Debugf("Current Block: %d Last Block: %d", currentBlock, lastBlock)
    }
}

func populateRecent(redisClient *redis.Client, block *BlockObject)(res bool, err error) {
    num, err := ParseQuantity(block.Number)

    if(err != nil) {
        fmt.Println("Failed to parse block number:", err);
        return false, err
    }

    log.Debugf("Inserting recent block for block #", num)

    var size int64 = 0
    timeStamp, _ := ParseQuantity(block.Timestamp)
    recentBlock := &blockwatcher.RecentBlock{
        Block: num,
        Timestamp: timeStamp,
        Miner: block.Miner,
    }
    blockJSON, _ := json.Marshal(recentBlock)
    size,err = redisClient.LPush(formatKey("recent_blocks"), blockJSON).Result()

    if(err != nil) {
        return false, err
    }
    if(size > int64(viper.GetInt("limits.recentBlocks"))) {
        redisClient.LTrim(formatKey("recent_blocks"),0,int64(viper.GetInt("limits.recentBlocks"))-1)
    }
    if(err == nil) {
        res = true
    }

    return
}

func populateMiner(block *BlockObject)(res bool, err error) {
    res = false
    num, _ := ParseQuantity(block.Number)
    gas, _ := ParseQuantity(block.GasUsed)
    difficulty, _ := ParseQuantity(block.Difficulty)

    timeStamp, _ := ParseQuantity(block.Timestamp)
    timeUnix := time.Unix(timeStamp, 0)
    date := fmt.Sprintf("%d-%d-%d", timeUnix.Year(), timeUnix.Month(), timeUnix.Day())

    miner := &blockwatcher.Miner{
        Block: num,
        Timestamp: timeStamp,
        Gas: gas,
        Difficulty: difficulty,
    }

    minerJSON, _ := json.Marshal(miner)

    RedisClient.Incr(formatKey("miner_history_"+block.Miner+"_"+date))
    RedisClient.Incr(formatKey("miner_history_"+block.Miner+"_"+date+"_"+strconv.Itoa(timeUnix.Hour())))
    RedisClient.Set(formatKey("block_hash_"+block.Hash), block.Hash, 0)
    RedisClient.LPush(formatKey("block_miner_"+block.Miner), minerJSON)
    populateBalance(block.Miner, block.Number) 
    res = true 

    return
}

func populateTransactions(redisClient *redis.Client, block *BlockObject)(res bool, err error) {
    res = false
    num, err := ParseQuantity(block.Number)

    if(err != nil) {
        log.Warnf("Failed to parse block number: %s", err)
        return false, err
    }
    var transactionCount, transactionIndex int64 = 0, 0

    transactionCount, err = EthGetBlockTransactionCountByNumber(block.Number)

    if(err != nil) {
        log.Warnf("Failed to retrieve transaction count for block #%n : %s", num, err)
        return false, err
    }

    timeStamp, _ := ParseQuantity(block.Timestamp)
    timeUnix := time.Unix(timeStamp, 0)
    date := fmt.Sprintf("%d-%d-%d", timeUnix.Year(), timeUnix.Month(), timeUnix.Day())
    log.Debugf("Importing %d transactions in block #%d", transactionCount, num)
    for transactionIndex < transactionCount {
        log.Debugf("Getting transaction idx %d out of %d", transactionIndex, transactionCount)
        var isContract int = 0;
        var txn *TransactionObject;
        txn, err := EthGetTransactionByBlockNumberAndIndex(block.Number, IntToQuantity(transactionIndex))
        if(err != nil) {
            fmt.Println("Failed to get transaction by index:",err)
            return false, err
        }

        // Sometimes I wish that I decided to do all of this conversion client side
        value, _ := ParseQuantityBig(txn.Value)
        if(txn.To == "") {
            receipt, _ := EthGetTransactionReceipt(txn.Hash)
            if(receipt != nil && receipt.ContractAddress != "") {
                log.Debugf("Transaction from %s was a contract execution at %s", txn.From, receipt.ContractAddress)
                txn.To = receipt.ContractAddress;
                isContract = 1;
            }
        }

        t := &blockwatcher.Transaction{
            Hash: txn.Hash,
            Timestamp: timeStamp,
            Value: value.String(),
            From: txn.From,
            To: txn.To,
            Number: num,
            Contract: isContract,
        }

        txnJson, _ := json.Marshal(t)

        redisClient.LPush(formatKey("txn_from_"+t.From), string(txnJson))
        redisClient.LPush(formatKey("txn_to_"+t.To), string(txnJson))

        var size int64 = 0

        size,err = redisClient.LPush(formatKey("recent_transactions"), string(txnJson)).Result()

        if(err != nil) {
            log.Warn("Failed to push new transaction: %s", err)
            return false, err
        }

        if(size > int64(viper.GetInt("limits.recentTransactions"))) {
            redisClient.LTrim(formatKey("recent_transactions"),0,int64(viper.GetInt("limits.recentTransactions"))-1)
        }

        //fmt.Println("FROM: ", t.From, " TO: ", t.To, " TXN: ", txn.Hash)
        populateBalance(t.To, block.Number)
        populateBalance(t.From, block.Number)
        redisClient.Incr(formatKey("txn_history_to_"+t.To+"_"+date))
        redisClient.Incr(formatKey("txn_history_from_"+t.From+"_"+date))
        redisClient.Incr(formatKey("txn_history_totals_"+date))
        redisClient.Incr(formatKey("txn_history_totals_"+date+"_"+strconv.Itoa(timeUnix.Hour())))

        transactionIndex++
    }

    res = true
    return
}

func populateBalance(address string, blockHeight string)(bool) {
   balanceWei, err := EthGetBalance(address, blockHeight)

   balanceDec, _ := decimal.NewFromString(balanceWei.String())
   balance := balanceDec.Div(decimal.New(1000000000000000000, 0))

   if(err != nil) {
       log.Errorf("Failed to get balance for %s: %s", address, err)
       return false
   }
   redisErr := RedisClient.RPush(formatKey("balance_"+address), balance.String()).Err()
   if(redisErr != nil) {
       log.Errorf("Failed to update balance for %s: %s", address, redisErr)
       return false
   } else {
       return true
   }
}

func populateUncles(block *BlockObject)(uncles int, err error) {
   num, _ := ParseQuantity(block.Number)
   uncles = len(block.Uncles)
   for index, uncle := range block.Uncles {
       fmt.Println("Insert uncle", uncle, uncle[index])
        _, err = RedisClient.LPush(formatKey("uncle_block_"+strconv.FormatInt(num, 10)), uncle[index]).Result()
   }
   return
}

func setLastBlock(redisClient *redis.Client, block *BlockObject)(num int64, err error) {
    num, err = ParseQuantity(block.Number)
    if(err != nil) {
        return 0, err
    }
    err = redisClient.Set(formatKey("last_block_id"), num, 0).Err()
    return
}

func getLastBlock(redisClient *redis.Client)(n int64) {
    res, err := redisClient.Get(formatKey("last_block_id")).Result()
    if(err != nil) {
        return 0;
    }
    n, err = strconv.ParseInt(res, 10, 64)
    return
}

func getCurrentBlockNumber()(n int64) {
    n, err := EthBlockNumber()
    if(err != nil) {
        log.Errorf("Error getting current block number: %s", err)
        n = 0;
    }
    return
}

func formatKey(key string)(string) {
    return viper.GetString("redis.prefix")+key
}
