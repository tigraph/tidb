package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: tigraph-n-degree <N> <src>")
		fmt.Println("This command is used to generate tht SQL of N degree of separation")
		return
	}

	n ,err:= strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("The first parameter should be a number")
		return
	}
	src := os.Args[2]

	fmt.Printf("SELECT * FROM people WHERE id IN (%s)\n", ndegree(n, src))
}

func ndegree(n int, src string)string {
	if n == 1 {
		return fmt.Sprintf("SELECT dst FROM friends WHERE src = %s", src)
	}

	subn := ndegree(n-1, src)
	return fmt.Sprintf("SELECT dst FROM friends WHERE src IN (%s) AND dst NOT IN (%s) AND src != %s", subn, subn, src)
}
