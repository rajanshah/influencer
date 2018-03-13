# -*- coding: utf-8 -*-

import os
import sys

from peewee import *

database = SqliteDatabase('/var/feed/data/bloggers/bloggers.db')

class BaseModel(Model):
    """A base model that will use our database
    """
    class Meta:
        database = database

class Author(BaseModel):
    blogger_id = UUIDField(unique=True)
    blogger_hash = CharField(unique=True)
    user_id = IntegerField(unique=True)
    name = CharField()
    short_name = CharField()
    firm_name = CharField()
    description = TextField()
    since = CharField()
    picture = BlobField(null=True)
    followers = IntegerField()
    ticker = TextField()
    url = CharField()
    updated_date = DateField()

class StyleCategory(BaseModel):
    author = ForeignKeyField(Author, related_name='styleCategory')
    style = CharField()
    category = CharField()

class Article(BaseModel):
    author = ForeignKeyField(Author, related_name='author')
    article_url = CharField()
    story_id = UUIDField(unique=True)
    updated_date = DateField()
