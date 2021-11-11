def transform_dummy(inputstr: str) -> str:
    return inputstr

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False