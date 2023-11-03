CREATE TEMP FUNCTION extract_string_value(
  event_params ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        string_value STRING,
        int_value INT64,
        float_value FLOAT64,
        double_value FLOAT64>
    >
  >,
  param_key STRING)
AS
(
  (SELECT
    value.string_value
  FROM UNNEST(event_params)
  WHERE key = param_key)
);

CREATE TEMP FUNCTION extract_int_value(
  event_params ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        string_value STRING,
        int_value INT64,
        float_value FLOAT64,
        double_value FLOAT64>
    >
  >,
  param_key STRING)
AS
(
  (SELECT
    value.int_value
  FROM UNNEST(event_params)
  WHERE key = param_key)
);

CREATE TEMP FUNCTION extract_float_value(
  event_params ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        string_value STRING,
        int_value INT64,
        float_value FLOAT64,
        double_value FLOAT64>
    >
  >,
  param_key STRING)
AS
(
  (SELECT
    IFNULL(value.float_value, value.double_value)
  FROM UNNEST(event_params)
  WHERE key = param_key)
);
