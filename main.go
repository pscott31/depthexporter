package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"code.vegaprotocol.io/vega/datanode/config"
	"code.vegaprotocol.io/vega/datanode/entities"
	"code.vegaprotocol.io/vega/logging"
	"code.vegaprotocol.io/vega/paths"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/holiman/uint256"
	"github.com/jackc/pgx/v4"
	"golang.org/x/exp/maps"
)

var BUCKET_MINUTES = 5

type Order struct {
	ID          entities.OrderID
	MarketID    entities.MarketID
	PartyID     entities.PartyID
	Side        entities.Side
	Price       uint256.Int
	Remaining   int64
	TimeInForce entities.OrderTimeInForce
	Type        entities.OrderType
	Status      entities.OrderStatus
}

func (o Order) isLive() bool {
	return o.Status == entities.OrderStatusActive
}

type Level struct {
	price uint256.Int
	side  entities.Side
}

func writeDepth(end time.Time, depth map[entities.MarketID]map[Level]int64) error {
	csvFile, err := os.Create(fmt.Sprintf("depth-%s.csv", end.Format("2006-01-02-15-04")))
	if err != nil {
		return fmt.Errorf("failed creating file: %w", err)
	}
	defer csvFile.Close()

	for marketID, prices := range depth {
		csvWriter := csv.NewWriter(csvFile)
		sprices := maps.Keys(prices)
		sort.Slice(sprices, func(i, j int) bool { return sprices[j].price.Lt(&sprices[i].price) })
		for _, level := range sprices {
			vol := prices[level]
			record := []string{
				end.Format(time.RFC3339),
				marketID.String(),
				level.side.String(),
				level.price.Dec(),
				strconv.FormatInt(vol, 10),
			}
			if err := csvWriter.Write(record); err != nil {
				return fmt.Errorf("failed to write to file: %w", err)
			}
		}
		csvWriter.Flush()
	}
	return nil
}

func writeLiveOrders(end time.Time, liveOrders map[entities.OrderID]Order) error {
	csvFile, err := os.Create(fmt.Sprintf("liveorders-%s.csv", end.Format("2006-01-02-15-04")))
	if err != nil {
		return fmt.Errorf("failed creating file: %w", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)

	for _, order := range liveOrders {
		record := []string{
			end.Format(time.RFC3339),
			order.MarketID.String(),
			order.PartyID.String(),
			order.ID.String(),
			order.Side.String(),
			order.Price.Dec(),
			strconv.FormatInt(order.Remaining, 10),
		}

		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}
	csvWriter.Flush()

	return nil
}

func doBucket(ctx context.Context, conn *pgx.Conn, start time.Time, end time.Time, liveOrders map[entities.OrderID]Order) error {
	query := `
	   select o.id, o.market_id, o.party_id, o.side, o.price, o.remaining, o.time_in_force, o.type, o.status
	     from orders o
	    where vega_time >= $1 and vega_time < $2
	 order by vega_time, seq_num`

	orders := []Order{}
	err := pgxscan.Select(ctx, conn, &orders, query, start, end)
	if err != nil {
		return fmt.Errorf("failed to query orders", err)
	}

	for _, order := range orders {
		if order.isLive() {
			liveOrders[order.ID] = order
		} else {
			delete(liveOrders, order.ID)
		}
	}

	depth := map[entities.MarketID]map[Level]int64{}
	for _, order := range liveOrders {
		if _, ok := depth[order.MarketID]; !ok {
			depth[order.MarketID] = map[Level]int64{}
		}
		lev := Level{price: order.Price, side: order.Side}
		depth[order.MarketID][lev] += order.Remaining
	}

	if err := writeDepth(end, depth); err != nil {
		return fmt.Errorf("failed to write depth csv: %w", err)
	}

	if err := writeLiveOrders(end, liveOrders); err != nil {
		return fmt.Errorf("failed to write live orders csv: %w", err)
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
