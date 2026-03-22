{
  points: {
    drop_races: 2,
    points: {
      default: [50, 41, 36, 33, 30, 27, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 3, 2, 1],
      major: [75, 64, 59, 55, 51, 48, 45, 42, 39, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 5, 4, 3, 2, 1],
    },
  },
  classes: [
    {
      name: 'Platinum',
      symbol: 'P',
      color: '#b4b8bb',
      drivers: [
        { name: 'Austin Cobb3' },
        { name: 'Chris Sherburn' },
        { name: 'Dale Green' },
        { name: 'David Fernandes' },
        { name: 'Derek M Cyphers' },
        { name: 'Derek Tirums' },
        { name: 'Ed Eijsenring' },
        { name: 'Greg Strelzoff' },
        { name: 'Jesse Lyon2' },
        { name: 'Leif Peterson' },
        { name: 'Lennie Holland' },
        { name: 'Logan Grado' },
        { name: 'Luke Mann' },
        { name: 'Marc Nistor' },
        { name: 'Matt Mosby' },
        { name: 'Matthew Siddall' },
        { name: 'Nick Facciolo' },
        { name: 'Nick Melaragno2' },
        { name: 'Rich Minkler' },
        { name: 'Robby Prescott' },
        { name: 'Ross Yost' },
        { name: 'Scott Dancer' },
        { name: 'William Wolfe4' },
      ],
    },
    {
      name: 'Gold',
      symbol: 'G',
      color: '#c9a84c',
      drivers: [
        { name: 'Casey Mcdonald' },
        { name: 'Enrico Gregoratto' },
        { name: 'George Poulos' },
        { name: 'James Franznick' },
        { name: 'Ken Eskridge' },
        { name: 'Peter Kummer' },
        { name: 'Peter Sigourney' },
        { name: 'Robert Galejs' },
        { name: 'Robert Neville' },
        { name: 'Rodney Campbell2' },
      ],
    },
  ],
  render: {
    combined_table: true,
    per_class_tables: true,
  },
  elo: {
    previous_seasons: [
      import '24_gt3.jsonnet',
      import '25-1_lmp2.jsonnet',
      import '25-2_gt4.jsonnet',
    ],
    min_races: 4,
    time_window: '365 days',
    weight: 0.5,
  },
  races: [
    {
      subsession_id: 82684096,
      race_name: 'Interlagos',
      penalties: [
        // Minkler
        { user_id: 48076, time: 5 },
        // Mann
        { user_id: 603983, time: 5 },
      ],
    },
    {
      subsession_id: 83060140,
      race_name: 'Road Atlanta',
      penalties: [
        { user_id: 335343, time: 5 },
        { user_id: 464635, time: 5 },
        { user_id: 622340, time: 5 },
      ],
    },
    {
      subsession_id: 83429093,
      race_name: 'Imola',
      penalties: [
        {
          user_id: 436882,  //Nevil
          time: 10,
        },
      ],
    },
    {
      subsession_id: 83800241,
      race_name: 'Sebring',
      penalties: [
        {
          user_id: 346566,
          time: 5,
        },
      ],
    },
    {
      subsession_id: 84164646,
      race_name: 'Road America',
      penalties: [
        // Logan
        { user_id: 622340, time: 5 },
        // Mann
        { user_id: 603983, time: 5 },
        // Logan
        { user_id: 1049877, time: 5 },
        // Mann
        { user_id: 721524, time: 5 },

      ],
    },
  ],
}
