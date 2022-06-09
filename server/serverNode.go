package main

import (
	"database/sql"
	"fmt"
	"net"
	pb "work/pb"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedServiceServerServer
}

const (
	host     = "127.0.0.1"
	database = "vending"
	user     = "search"
	password = "123456"
	root     = "root"
	root_pwd = "s850429s"
)

func (s *server) SearchAll(ctx context.Context, in *pb.GetProductRequest) (*pb.GetAllProductReply, error) {
	logrus.Printf("查詢所有產品")
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3307)/%s?allowNativePasswords=true", user, password, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.Printf("Could not connect to MySQL: %v\n", err.Error())
		return &pb.GetAllProductReply{}, err
	}
	defer db.Close()
	rows, err := db.Query("select * from product_info")
	defer rows.Close()
	if err != nil {
		logrus.Printf("Could not search")
	}
	mapInstances := make(map[int32]string)
	for rows.Next() {
		var uid int
		var name string
		var price int
		var inventory int
		var status int
		var update_time []uint8

		err = rows.Scan(&uid, &name, &price, &inventory, &status, &update_time)
		if err != nil {
			return &pb.GetAllProductReply{}, err
		}
		uid2 := int32(uid)
		mapInstances[uid2] = name
	}
	return &pb.GetAllProductReply{AllProduct: mapInstances}, nil
}

func (s *server) Search(ctx context.Context, in *pb.GetProductRequest) (*pb.GetProductReply, error) {
	logrus.Printf("查詢產品ID :%v\n", in.Id)
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3307)/%s?allowNativePasswords=true", user, password, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.Printf("Could not connect to MySQL: %v\n", err.Error())
		return &pb.GetProductReply{}, err
	}
	defer db.Close()
	var price, inventory int32
	var product string
	err = db.QueryRow("select name,price,inventory FROM product_info where id=?", in.Id).Scan(&product, &price, &inventory)
	if err != nil {
		logrus.Warnf(err.Error())
		return &pb.GetProductReply{}, err
	}
	return &pb.GetProductReply{Name: product, Price: price, Inventory: inventory}, nil

}

func (s *server) InsertProduct(ctx context.Context, in *pb.InsertRequest) (*pb.StatusReply, error) {
	logrus.Printf("新增商品 :%v\n", in.Name)
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true", root, root_pwd, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.Printf("Could not connect to MySQL: %v\n", err.Error())
		return &pb.StatusReply{}, err
	}
	defer db.Close()
	var status int
	if in.Inventory == 0 {
		status = 1
	} else {
		status = 0
	}
	_, err1 := db.Exec("insert into product_info(name,price,inventory,status,update_time) values(?,?,?,?,now())", in.Name, in.Price, in.Inventory, status)
	if err1 != nil {
		logrus.Printf(err1.Error())
		return &pb.StatusReply{Code: -1, Status: "Fail"}, err1
	}
	return &pb.StatusReply{Code: 1, Status: "Success"}, nil
}

func (s *server) ModifyProduct(ctx context.Context, in *pb.ModifyRequest) (*pb.StatusReply, error) {
	logrus.Printf("修改商品 :%v\n", in.Name)
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true", root, root_pwd, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.Printf("Could not connect to MySQL: %v\n", err.Error())
		return &pb.StatusReply{}, err
	}
	defer db.Close()
	var status int
	if in.Inventory == 0 {
		status = 1
	} else {
		status = 0
	}
	_, err1 := db.Exec("update product_info set name=?,price=?,inventory=?,status=? where id=?", in.Name, in.Price, in.Inventory, status, in.Id)
	if err1 != nil {
		logrus.Printf(err1.Error())
		return &pb.StatusReply{Code: -1, Status: "Fail"}, err1
	}
	return &pb.StatusReply{Code: 1, Status: "Success"}, nil
}

func (s *server) BuyProduct(ctx context.Context, in *pb.BuyRequest) (*pb.StatusReply, error) {
	logrus.Printf("購買人：%v, 購買商品 :%v\n", in.Customer, in.Production)
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true", root, root_pwd, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.Printf("Could not connect to MySQL: %v\n", err.Error())
		return &pb.StatusReply{}, err
	}
	defer db.Close()
	price, inventory := searchPriceAndInventory(in.Production)
	_, err1 := db.Exec("insert into sale_info(customer,production,price,update_time) values(?,?,?,now())", in.Customer, in.Production, price)
	if err1 != nil {
		return &pb.StatusReply{Code: -1, Status: "訂單成立失敗"}, err1
	}

	var status int
	if inventory-1 == 0 {
		status = 1
	} else {
		status = 0
	}
	_, err2 := db.Exec("update product_info set inventory=?,status=? where name=?", inventory-1, status, in.Production)
	if err2 != nil {
		logrus.Printf(err2.Error())
		return &pb.StatusReply{Code: -1, Status: "Fail"}, err1
	}
	return &pb.StatusReply{Code: 1, Status: "Success"}, nil
}

func (s *server) TurnoverSearch(ctx context.Context, in *pb.TurnoverRequest) (*pb.TurnoverReply, error) {
	logrus.Printf("查詢日期:%v\n", in.Date)
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3307)/%s?allowNativePasswords=true", user, password, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.Printf("Could not connect to MySQL: %v\n", err.Error())
		return &pb.TurnoverReply{}, err
	}
	defer db.Close()
	date := in.Date
	today_sale := "select production,count(*),SUM(price) from sale_info where update_time BETWEEN '" + date + " 00:00:59' AND '" + date + " 23:59:59' GROUP BY production;"
	//today_sale := "select production,count(*),SUM(price) from sale_info GROUP BY production;"
	rows, _ := db.Query(today_sale)
	defer rows.Close()
	var total_price int
	mapInstances := make(map[string]int32)
	for rows.Next() {
		var production string
		var sale_count int
		var price int
		_ = rows.Scan(&production, &sale_count, &price)
		mapInstances[production] = int32(sale_count)
		total_price += price
	}
	return &pb.TurnoverReply{ProductInfo: mapInstances, TotalPrice: int32(total_price)}, nil
}

func main() {
	logrus.Infof("Master node start")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logrus.Fatalf("Can't listen on port %v", err.Error())
	}
	s := grpc.NewServer()
	pb.RegisterServiceServerServer(s, &server{})
	reflection.Register(s)

	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("Can't init gRPC server：%v", err.Error())
	}

}

func searchPriceAndInventory(product string) (int32, int32) {
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3307)/%s?allowNativePasswords=true", user, password, host, database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		fmt.Println("connect MySQL failed", err)
		return 0, 0
	}
	var price, inventory int
	err = db.QueryRow("select price,inventory FROM product_info where name=?", product).Scan(&price, &inventory)
	if err != nil {
		return 0, 0
	}
	return int32(price), int32(inventory)
}
