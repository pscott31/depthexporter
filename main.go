package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"code.vegaprotocol.io/vega/datanode/config"
	"code.vegaprotocol.io/vega/datanode/entities"
	"code.vegaprotocol.io/vega/logging"
	"code.vegaprotocol.io/vega/paths"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
	"golang.org/x/exp/maps"
)

var BUCKET_MINUTES = 1

type Order struct {
	ID          entities.OrderID
	MarketID    entities.MarketID
	Side        entities.Side
	Price       decimal.Decimal
	Remaining   int64
	TimeInForce entities.OrderTimeInForce
	Type        entities.OrderType
	Status      entities.OrderStatus
}

func (o Order) isLive() bool {
	if o.Status != entities.OrderStatusActive && o.Status != entities.OrderStatusParked {
		return false
	}
	if o.Type != entities.OrderTypeLimit {
		return false
	}
	if o.TimeInForce == entities.OrderTimeInForceIOC || o.TimeInForce == entities.OrderTimeInForceFOK {
		return false
	}
	return true
}

type Level struct {
	price decimal.Decimal
	side  entities.Side
}

func doBucket(ctx context.Context, conn *pgx.Conn, start time.Time, end time.Time, liveOrders map[entities.OrderID]Order) error {
	query := `
	   select o.id, o.market_id, o.side, o.price, o.remaining, o.time_in_force, o.type, o.status
	     from orders o
	    where vega_time >= $1 and vega_time < $2
	 order by vega_time, seq_num`

	orders := []Order{}
	err := pgxscan.Select(ctx, conn, &orders, query, start, end)
	if err != nil {
		log.Fatal("failed to query orders", logging.Error(err))
	}

	for _, order := range orders {
		if order.isLive() {
			liveOrders[order.ID] = order
		} else {
			delete(liveOrders, order.ID)
		}
	}

	volume := map[entities.MarketID]map[Level]int64{}
	for _, order := range liveOrders {
		if _, ok := volume[order.MarketID]; !ok {
			volume[order.MarketID] = map[Level]int64{}
		}
		lev := Level{price: order.Price, side: order.Side}
		volume[order.MarketID][lev] += order.Remaining
	}

	for marketID, prices := range volume {
		csvFile, err := os.Create(fmt.Sprintf("depth-%s-%s.csv", marketID, end.Format("2006-01-02-15-04")))
		if err != nil {
			log.Fatal("failed creating file: %s", logging.Error(err))
		}
		defer csvFile.Close()
		csvWriter := csv.NewWriter(csvFile)

		// todo side
		sprices := maps.Keys(prices)
		sort.Slice(sprices, func(i, j int) bool { return sprices[j].price.LessThan(sprices[i].price) })
		for _, level := range sprices {
			vol := prices[level]
			record := []string{
				marketID.String(),
				end.Format(time.RFC3339),
				level.side.String(),
				level.price.String(),
				strconv.FormatInt(vol, 10),
			}
			csvWriter.Write(record)
		}
		csvWriter.Flush()
	}
	fmt.Printf("bucket ending %s, orders in bucket: %d, live orders at end: %d\n",
		end,
		len(orders),
		len(liveOrders))
	return nil
}

type TimeRow struct {
	Time time.Time
}

func getFirstBlockBucketStart(ctx context.Context, conn *pgx.Conn) (time.Time, error) {
	ret := TimeRow{}
	q := fmt.Sprintf("select time_bucket( '%d minutes', (select vega_time from blocks order by vega_time limit 1)) as time", BUCKET_MINUTES)
	err := pgxscan.Get(ctx, conn, &ret, q)
	return ret.Time, err
}

func getLastBlockTime(ctx context.Context, conn *pgx.Conn) (time.Time, error) {
	ret := TimeRow{}
	q := "select vega_time as time from blocks order by vega_time desc limit 1"
	err := pgxscan.Get(ctx, conn, &ret, q)
	return ret.Time, err
}

func main() {
	log := logging.NewLoggerFromConfig(
		logging.NewDefaultConfig()).Named("depth-exporter")
	vegaPaths := paths.New("")

	configWatcher, err := config.NewWatcher(context.Background(), log, vegaPaths)
	if err != nil {
		log.Fatal("failed to create config watcher", logging.Error(err))
	}
	cfg := configWatcher.Get()

	ctx := context.Background()
	connStr := cfg.SQLStore.ConnectionConfig.GetConnectionString()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal("failed to connect to database", logging.Error(err))
	}

	start, err := getFirstBlockBucketStart(ctx, conn)
	if err != nil {
		log.Fatal("failed to get first block", logging.Error(err))
	}
	end := start.Add(time.Minute * time.Duration(BUCKET_MINUTES))
	liveOrders := map[entities.OrderID]Order{}

	for {
		lastBlockTime, err := getLastBlockTime(ctx, conn)
		if err != nil {
			log.Fatal("failed to get last block", logging.Error(err))
		}

		if end.After(lastBlockTime) {
			log.Info("done")
			return
		}

		err = doBucket(ctx, conn, start, end, liveOrders)
		if err != nil {
			log.Fatal("failed to process bucket", logging.Error(err))
		}
		start = end
		end = start.Add(time.Minute * time.Duration(BUCKET_MINUTES))
	}
}
