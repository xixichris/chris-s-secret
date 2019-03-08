from orm import Model, StringField, BooleanField, FloatField, TextField

class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=True)
