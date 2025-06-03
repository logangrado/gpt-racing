{
  points: {
    drop_races: 1,
    points: {
      default: [25, 21, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      large: [32, 27, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      major: [38, 32, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
    },
  },
  elo: {
    previous_seasons: [
      import '24_gt3.jsonnet',
      import '25-1_lmp2.jsonnet',
    ],
    min_races: 4,
    time_window: '365 days',
  },
  races: [
    {
      subsession_id: 77351364,
      race_name: 'Summit',
    },
  ],
}
