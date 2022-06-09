package main

import (
	"encoding/json"
	pb "work/pb"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	address = "127.0.0.1:50051"
)

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logrus.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	searchProduct := pb.NewServiceServerClient(conn)                                               //起Client Server
	resultSearchAll, err := searchProduct.SearchAll(context.Background(), &pb.GetProductRequest{}) //查所有
	if err != nil {
		logrus.Printf("Can't execute")
	} else {
		resultJson, _ := json.Marshal(resultSearchAll)
		logrus.Printf("Reply [SearchProduct]：%s", resultJson)
	}

	resultSearchOne, err := searchProduct.Search(context.Background(), &pb.GetProductRequest{Id: 9}) //查單一
	if err != nil {
		logrus.Printf("Can't execute")
	} else {
		resultJson2, _ := json.Marshal(resultSearchOne)
		logrus.Printf("Reply [SearchProduct]：%s", resultJson2)
	}

	name := "烤肉炒麵"
	InsertProduct, err := searchProduct.InsertProduct(context.Background(), &pb.InsertRequest{Name: name, Price: 60, Inventory: 10}) //新增
	if err != nil {
		logrus.Printf("Can't Insert Product")
	} else {
		resultJson2, _ := json.Marshal(InsertProduct)
		logrus.Printf("Insert Status：%s", resultJson2)
	}

	ModifyProduct, err := searchProduct.ModifyProduct(context.Background(), &pb.ModifyRequest{Name: "烤肉沙拉", Price: 55, Inventory: 20, Id: 17}) //修改
	if err != nil {
		logrus.Printf("Can't Modify Product")
	} else {
		resultJson2, _ := json.Marshal(ModifyProduct)
		logrus.Printf("Insert Status：%s", resultJson2)
	}

	BuyProduct, err := searchProduct.BuyProduct(context.Background(), &pb.BuyRequest{Customer: "George", Production: "肉燥飯"}) //客人購買商品
	if err != nil {
		logrus.Printf("Can't Buy Product")
	} else {
		resultJson2, _ := json.Marshal(BuyProduct)
		logrus.Printf("Insert Status：%s", resultJson2)
	}

	Turnover, err := searchProduct.TurnoverSearch(context.Background(), &pb.TurnoverRequest{Date: "2022-06-08"}) //單日報表
	if err != nil {
		logrus.Printf("Can't Search")
	} else {
		resultJson2, _ := json.Marshal(Turnover)
		logrus.Printf("Insert Status：%s", resultJson2)
	}
}
