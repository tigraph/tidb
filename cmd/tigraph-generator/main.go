package main

import (
	"fmt"
	"os"

	"github.com/Pallinder/go-randomdata"
)

const RowCount = 100000
const friendsMin = 30
const friendsMax = 100
const step = 10000

func buildVertex(f *os.File) error {
	var name string
	sql := "insert into people values "
	for i := 0; i < RowCount; i++ {
		k := randomdata.Number(10, 20)
		if k%2 == 0 {
			name = randomdata.FullName(randomdata.Male)
		} else {
			name = randomdata.FullName(randomdata.Female)
		}
		sql += fmt.Sprintf(" (%d, '%s'),", i+1, name)

		if (i+1)%step == 0 {
			sql = sql[:len(sql)-1]
			sql += ";\n"
			_, err := f.WriteString(sql)
			if err != nil {
				return err
			}
			sql = "insert into people values "
		}
	}

	return nil
}

func buildEdge(f *os.File) error {
	cnt := 0
	sql := fmt.Sprintf("insert ignore into friends values ")
	for i := 0; i < RowCount; i++ {
		friendsCnt := randomdata.Number(friendsMin, friendsMax+1)
		for j := 0; j < friendsCnt; j++ {
			cnt++
			to := randomdata.Number(1, RowCount+1)
			if to == i {
				continue
			}
			sql = sql + fmt.Sprintf(" (%d, %d),", i+1, to)

			if (cnt+1)%step == 0 {
				sql = sql[:len(sql)-1]
				sql += ";\n"
				_, err := f.WriteString(sql)
				if err != nil {
					return err
				}
				sql = "insert ignore into friends values "
			}
		}
	}

	if (cnt+1)%step != 0 {
		sql = sql[:len(sql)-1]
		sql += ";\n"
		_, err := f.WriteString(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildGraph() {
	file, err := os.OpenFile("./data.sql", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("file open error")
		return
	}

	_, err = file.WriteString("drop table if exists people;\n")
	if err != nil {
		os.Exit(1)
	}
	_, err = file.WriteString("create tag people (id bigint, name varchar(32));\n")
	if err != nil {
		os.Exit(1)
	}
	_, err = file.WriteString("drop table if exists friends;\n")
	if err != nil {
		os.Exit(1)
	}
	_, err = file.WriteString("create edge friends (src bigint, dst bigint);\n")
	if err != nil {
		os.Exit(1)
	}
	err = buildVertex(file)
	if err != nil {
		os.Exit(1)
	}
	err = buildEdge(file)
	if err != nil {
		os.Exit(1)
	}
}

func buildPeopleRel(f *os.File) error {
	var name string
	for i := 0; i < RowCount; i++ {
		k := randomdata.Number(10, 20)
		if k%2 == 0 {
			name = randomdata.FullName(randomdata.Male)
		} else {
			name = randomdata.FullName(randomdata.Female)
		}
		sql := fmt.Sprintf("insert into people_rel values(%d, \"%s\");\n", i+1, name)
		_, err := f.WriteString(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildFriendsRel(f *os.File) error {
	for i := 0; i < RowCount; i++ {
		for j := 0; j <= 10; j++ {
			to := randomdata.Number(1, RowCount+1)
			if to == i {
				continue
			}
			sql := fmt.Sprintf("insert ignore into friends values(%d, %d);\n", i+1, to)
			_, err := f.WriteString(sql)
			if err != nil {
				return err
			}
			sql = fmt.Sprintf("insert ignore into friends values(%d, %d);\n", to, i+1)
			_, err = f.WriteString(sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func buildRal() {
	file, err := os.OpenFile("./data_rel.sql", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("file open error")
		return
	}

	_, err = file.WriteString("create table people_rel (id bigint, name varchar(32));\n")
	if err != nil {
		os.Exit(1)
	}
	_, err = file.WriteString("create table friends_rel (`from` bigint, `to` bigint);\n")
	if err != nil {
		os.Exit(1)
	}
	err = buildPeopleRel(file)
	if err != nil {
		os.Exit(1)
	}
	err = buildFriendsRel(file)
	if err != nil {
		os.Exit(1)
	}
}

func main() {
	buildGraph()
}
