
/* task1 : count names by origin */
SELECT ori, COUNT(prenom) FROM prenoms LATERAL VIEW explode(origin) originsList AS ori GROUP BY ori;

/* task2 : count number of names by number of origins */
SELECT SIZE(origin), COUNT(prenom) FROM prenoms GROUP BY SIZE(origin);

/* task3 : count percent of names by gender */
SELECT gen, COUNT(prenom) FROM prenoms LATERAL VIEW EXPLODE (gender) gendersList AS gen GROUP BY gen;
