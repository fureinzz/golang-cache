package cache

import (
	"sync"
	"time"
)

/*
 * Необходимо написать in-memory кэш, который будет по ключу (UUID пользователя)
 * возвращать профиль и список его заказов.
 *
 * 1. У кэша должен быть TTL (N-секунд)
 * 2. Кэшем может пользоваться функция(-и), которая работает с заказами (добавляет/обновляет/удаляет). Если TTL кэша истек, то возрашается "NULL" значение. При обновлении TTL снова устанавлиается N-сек. Методы должны быть потокобезопасными.
 * 3. Должны быть написаны тестовые сценарии использования данного кэша (базовые стркутуры менять нельзя).
 *
 * Допольнительное задание: Реальизовать автоматическую очистку истекших записей кэша.
 */

type Profile struct {
	UUID   string
	Name   string
	Orders []*Order
}

type Order struct {
	UUID      string
	Value     interface{}
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Cache struct {
	ttl   time.Duration
	data  map[string]*CacheItem
	mutex sync.RWMutex
}

type CacheItem struct {
	profile  *Profile
	expireAt time.Time
}

// Функция-конструктор для создания единицы кэш-хранилища. Параллельно с созданием кэша
// запускаем сборщик мусора, который каждые K-секунд очищает хранилище от протухших значений.
func New(ttl time.Duration) *Cache {
	cache := &Cache{
		data:  make(map[string]*CacheItem),
		ttl:   ttl,
		mutex: sync.RWMutex{},
	}

	go cache.GarbageCollector()

	return cache
}

/*
 * Функция получения значения кэша по уникальному идентификатору `UUID`
 */
func (cache *Cache) Get(UUID string) (*Profile, bool) {
	// На время действия функции получения значения
	// блокируем мьютекс на чтение кэш-хранилища
	cache.mutex.RLock()

	// При завершении функции получения значения снимаем
	// блокировку с мьютекса на чтения хранилища
	defer cache.mutex.RUnlock()

	item, ok := cache.data[UUID]

	if !ok {
		return nil, false
	}

	// В случае если значение кэша просрочено возвращаем нулево значение
	if time.Now().After(item.expireAt) {
		return nil, false
	}

	return item.profile, true
}

/*
 * Функция записи значения в кэш-хранилище
 */
func (cache *Cache) Set(profile *Profile) {
	// На время действия функции записи значения
	// блокируем мьютекс на запись в кэш-хранилище
	cache.mutex.Lock()

	// При завершении функции снимаем блокировку с мьютекса
	// на запись значений в кэш-хранилище
	defer cache.mutex.Unlock()

	// Устанавливаем/обновляем время истечения кэша
	expireAt := time.Now().Add(cache.ttl)

	cache.data[profile.UUID] = &CacheItem{
		profile:  profile,
		expireAt: expireAt,
	}
}

/*
 * Оптимизация функции: Есть возможность оптимизировать время для взаимодействия с хэш-хранилищем во время
 * выполнения процедуры следующим образом - Вместо блокировки мьютекса на запись, блокируем мьютекс на чтение
 * и собираем ID каждой просроченной записи кэша в отделный срез с помощью метода `append`. После окончательного
 * сбора всех идентификаторов просроченных записей начинаем очистку и паралелльно блокируем мьютекс на запись значений.
 *
 * Путем подобной оптимизации можем позволить другим тредам
 */
func cleanCacheItems(cache *Cache) {
	// До момента сбора идентификаторов протухших кэш-значений блокируем мьютекс на чтение
	// из кэш-хранилища, поскольку может возникнуть конфликт при прочтении удаляемого значения
	cache.mutex.RLock()

	// При завершении выполении функции снимаем блокировку с мьютекса и разрешаем
	// запись и создание новых кэш-значений
	defer cache.mutex.Unlock()

	// Срез идентификаторов истекших по времени кэш-значений.
	expiredCacheItemIds := make([]string, len(cache.data))

	// В данном цикле исключител ьно ищем истекшие по времени хэш-значения и
	// помещаем и в срез для последующего удаления
	for id, item := range cache.data {
		isCacheItemExpired := time.Now().After(item.expireAt)

		if isCacheItemExpired {
			expiredCacheItemIds = append(expiredCacheItemIds, id)
		}
	}

	// Снимаем блокировку мьютекса после сбора всех идентификаторов протухших
	// кэш-значений и обновляем его на чтение до момента удаления всех собранных кэшей
	cache.mutex.RUnlock()
	cache.mutex.Lock()

	// Удаляем из кэша все истекшие по времени значения
	for _, id := range expiredCacheItemIds {
		delete(cache.data, id)
	}
}

func (cache *Cache) GarbageCollector() {
	// Запускаем сборщик мусора, который срабатывает каждые N-секунд
	// по интервалу и удаляет значения из кэш-хранилища. В данном случае интервал
	// срабатывает каждую минуту. Чем больше интервал по очистке хранилища, тем больше памяти оно начинает занимать
	ticker := time.NewTicker(time.Minute)

	// При завершении очистки закрываем интервал
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cleanCacheItems(cache)
		}
	}
}
