from databases import Database

database = Database("postgresql://postgres:@localhost/nassync", min_size=1, max_size=5)

