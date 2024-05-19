package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	// 1. Функция Generator
	var i int64 = 1
	for {
		select {
		case ch <- i:
			fn(i)
			i++
		case <-ctx.Done():
			close(ch)
			return
		}
	}

}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {
	// 2. Функция Worker
	defer close(out)
	for i := range in {
		out <- i
	}
}

func main() {
	chIn := make(chan int64)

	// 3. Создание контекста

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum atomic.Int64   // сумма сгенерированных чисел
	var inputCount atomic.Int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму

	go Generator(ctx, chIn, func(i int64) {
		inputSum.Add(i)
		inputCount.Add(1)
	})

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// 4. Собираем числа из каналов outs
	var i int64
	for i = 0; i < int64(NumOut); i++ {
		wg.Add(1)
		go func(in <-chan int64, i int64) {
			for value := range in {
				chOut <- value
				amounts[i]++
			}
			wg.Done()
		}(outs[i], i)

	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count atomic.Int64 // количество чисел результирующего канала
	var sum atomic.Int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала

	for value := range chOut {
		count.Add(1)
		//sum += value
		sum.Add(value)
	}

	fmt.Println("Количество чисел", inputCount.Load(), count.Load())
	fmt.Println("Сумма чисел", inputSum.Load(), sum.Load())
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов

	if inputSum.Load() != sum.Load() {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum.Load(), sum.Load())
	}
	if inputCount.Load() != count.Load() {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount.Load(), count.Load())
	}
	for _, v := range amounts {
		inputCount.Add(-v)
	}
	if inputCount.Load() != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
