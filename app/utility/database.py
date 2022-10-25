from databases import Database

database = Database("postgresql://postgres:@localhost/Barrier-Auth", min_size=1, max_size=10)