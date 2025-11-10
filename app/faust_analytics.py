import faust

app = faust.App(
    'view-counter-app',
    broker='kafka://kafka:9092',
)

class BookView(faust.Record):
    book_id: str
    views: int = 1  

topic = app.topic('book_views', value_type=BookView)
views_per_book = app.Table('views_per_book', default=int)

@app.agent(topic)
async def process_views(stream):
    async for event in stream:
        # Если views не пришло, считаем 1 просмотр
        views_count = event.views if hasattr(event, 'views') else 1
        
        views_per_book[event.book_id] += views_count
        print(f'[FaustAnalytics] Book {event.book_id}: +{views_count} views, total: {views_per_book[event.book_id]}')

@app.timer(interval=5.0)
async def print_stats():
    print('\n=== Current Statistics ===')
    
    items_count = 0
    for key in list(views_per_book.keys()):
        count = views_per_book[key]
        if count > 0:
            print(f'Book {key}: {count} views')
            items_count += 1
    
    if items_count == 0:
        print('No view data available')
    
    print('==========================\n')

if __name__ == '__main__':
    app.main()