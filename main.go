package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"net/http"
	"encoding/json"
	"log"
	"strconv"
	"reflect"
	"github.com/pborman/uuid"
	"context"
	"cloud.google.com/go/storage"
	"cloud.google.com/go/bigtable"
	"io"
      	"github.com/auth0/go-jwt-middleware"
      	"github.com/dgrijalva/jwt-go"
      	"github.com/gorilla/mux"

)

const (
	BUCKET_NAME = "post-images-253602"
	DISTANCE = "200km"
	INDEX = "around"
	TYPE = "post"
	ES_URL = "http://34.70.202.237:9200"
	PROJECT_ID = "around-253602"
	BT_INSTANCE = "around-post"
)

// secrete value , we can change it to anything we want
var mySigningKey = []byte("secret")

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User 		string 	`json:"user"`
	Message 	string 	`json:"message"`
	Location 	Location `json:"location"`
	Url 		string 	`json:"url"`
}

func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("start-service")
	
	r := mux.NewRouter()
	
	// jwtMiddleware is a verification method
	// in old version code: "http://HandleFunc('/post',handlerPost)"
	// when we accept post request and we will handle this request directly
	// in new version code: "r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")"
	// when we accept post request, we will first user the jwtMiddleware to test whether the token is the same or not and if the token is a perfect match, we will handle the request like old time. but if it goes south, it wil return err	
	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter : func(token *jwt.Token)(interface{},error){
			return mySigningKey , nil
	},
		SigningMethod:jwt.SigningMethodHS256,
	})
 
	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
      	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
      	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

      	http.Handle("/", r)
      	log.Fatal(http.ListenAndServe(":8080", nil))

}

func handlerPost(w http.ResponseWriter, r *http.Request){
	user := r.Context().Value("user")
      	claims := user.(*jwt.Token).Claims
      	username := claims.(jwt.MapClaims)["username"]

	w.Header().Set("Content-Type","application/json")
	w.Header().Set("Access-Control-Allow-Origin","*")
	w.Header().Set("Access-Control-Allow-Headers","Content-Type,Authorization")
	
	r.ParseMultipartForm(32 << 20)

	// Parse from data
	fmt.Println("Received one post request %s \n", r.FormValue("message"))
	lat , _ := strconv.ParseFloat(r.FormValue("lat"),64)
	lon , _ := strconv.ParseFloat(r.FormValue("lon"),64)

	p := &Post{
		User:    username.(string),
		Message : r.FormValue("message"),
		Location : Location{
			Lat : lat,
			Lon : lon,
		},
	}
	
      	id := uuid.New()

	file , _ , err := r.FormFile("image")
	if err != nil{
		http.Error(w, "GCS is not setup",http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v \n", err)
		panic(err)
	}
	defer file.Close()

	ctx := context.Background()

	_ , attrs, err := saveToGCS(ctx , file , BUCKET_NAME, id)
	if err != nil{
		http.Error(w , "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v \n", err)
		panic(err)
	}

	p.Url = attrs.MediaLink
	
      	// Save to ES.
      	saveToES(p, id)
	
	// Save to big table
	saveToBigTable(p,id)
}

func saveToBigTable(p *Post , id string) {
	ctx := context.Background()
	// you must update project name here
	bt_client , err := bigtable.NewClient(ctx , PROJECT_ID , BT_INSTANCE)
	if err != nil {
		panic(err)
		return 
	}	

	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	t := bigtable.Now()
	
	mut.Set("post", "user" , t , []byte(p.User))
	mut.Set("post", "message", t, []byte(p.Message)) 
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))
 
	err = tbl.Apply(ctx, id, mut) 
	if err != nil {
 		panic(err)
		return
	} 
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
}

func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle , *storage.ObjectAttrs, error){
	client , err := storage.NewClient(ctx)
	if err != nil{
		return nil , nil , err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	// Next check if the bucket exists
	if _ , err = bucket.Attrs(ctx); err != nil {
		return nil , nil , err
	}

	obj := bucket.Object(name)
	w := obj.NewWriter(ctx)
	if _ , err := io.Copy(w,r);err != nil{
		return nil , nil , err
	}
	if err := w.Close(); err != nil{
		return nil , nil , err
	}

	if err := obj.ACL().Set(ctx , storage.AllUsers , storage.RoleReader); err != nil{
		return nil , nil , err
	}

	attrs , err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n" , attrs.MediaLink)
	return obj , attrs , err
}

func saveToES(p *Post , id string){
	es_client , err := elastic.NewClient(elastic.SetURL(ES_URL),elastic.SetSniff(false))
	if err != nil{
		panic(err)
	}

	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil{
		panic(err)
	}

	fmt.Printf("Post is saved to index: %s\n",p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request){
	fmt.Println("Received one request for search")
	lat,_ := strconv.ParseFloat(r.URL.Query().Get("lat"),64)
	lon,_ := strconv.ParseFloat(r.URL.Query().Get("lon"),64)

	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != ""{
		ran = val + "km"
	}

	fmt.Fprintf(w,"Search reveived: %s %s",lat,lon)

	client , err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil{
		panic(err)
	}	

	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()

	if err != nil{
		panic(err)
	}

	fmt.Println("Query took %d milliseconds \n",searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts \n", searchResult.TotalHits())

	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)){
		p := item.(Post)
		fmt.Printf("Post by %s: %s at lat %v and lon %v \n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		ps = append(ps , p)
	}
	
	js , err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type" , "application/json")
	w.Header().Set("Access-Control-Allow-Origin" , "*")
	w.Write(js)
}
