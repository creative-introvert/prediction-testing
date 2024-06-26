import {Command, Options} from '@effect/cli';
import * as PT from '@creative-introvert/tons-of-tests';

import * as P from './prelude.js';
import {Config} from './Config.js';
import {getPreviousTestRunResults, cached} from './common.js';

const LabelSchema = P.Schema.transform(
    P.Schema.String,
    P.Schema.Array(
        P.Schema.String.pipe(P.Schema.compose(PT.Classify.LabelSchema)),
    ),
    {
        decode: s => s.split(','),
        encode: xs => xs.join(','),
    },
);

const labels = Options.text('labels').pipe(
    Options.withSchema(LabelSchema),
    Options.optional,
    Options.withDescription('Filter labels (OR).'),
);

const TagsSchema = P.Schema.transform(
    P.Schema.String,
    P.Schema.Array(P.Schema.String),
    {
        decode: s => (s.length === 0 ? [] : s.split(',')),
        encode: xs => xs.join(','),
    },
);

const orTags = Options.text('tags').pipe(
    Options.withSchema(TagsSchema),
    Options.optional,
    Options.withDescription('Filter tags (OR).'),
);

const andTags = Options.text('all-tags').pipe(
    Options.withSchema(TagsSchema),
    Options.optional,
    Options.withDescription('Filter tags (AND).'),
);


// TEST: summarize --labels doesn't affect the db (i.e. same test results are stored)
// TEST: summarize --run -> commit -> summarize --run is idempotent
export const _sumarize = <I = unknown, O = unknown, T = unknown>({
    labels: maybeLabels,
    orTags: maybeOrTags,
    andTags: maybeAndTags,
    cached,
    config: {testSuite, displayConfig, concurrency},
}: {
    labels: P.Option.Option<readonly PT.Classify.Label[]>;
    orTags: P.Option.Option<readonly string[]>;
    andTags: P.Option.Option<readonly string[]>;
    cached: boolean;
    config: Config<I, O, T>;
}) =>
    P.Effect.gen(function* () {
        const tests = yield* PT.TestRepository.TestRepository;
        yield* P.Effect.logDebug('repository');

        const hasLabel = (label: PT.Classify.Label) =>
            P.Option.isNone(maybeLabels) || maybeLabels.value.includes(label);

        const hasOrTags = (tags: readonly string[]) =>
            P.Option.isNone(maybeOrTags) ||
            tags.some(tag => maybeOrTags.value.includes(tag));

        const hasAndTags = (tags: readonly string[]) =>
            P.Option.isNone(maybeAndTags) ||
            maybeAndTags.value.every(tag => tags.includes(tag));

        const filter = (
            args: PT.Test.TestRunResults,
        ): PT.Test.TestRunResults => {
            const {testCaseHashes, testResultsByTestCaseHash, ...rest} = args;
            const _testCaseHashes: string[] = [];
            const _testResultsByTestCaseHash: PT.Test.TestRunResults['testResultsByTestCaseHash'] =
                {};

            for (const hash of testCaseHashes) {
                const next = testResultsByTestCaseHash[hash];
                if (
                    hasLabel(next.label) &&
                    hasOrTags(next.tags) &&
                    hasAndTags(next.tags)
                ) {
                    _testCaseHashes.push(hash);
                    _testResultsByTestCaseHash[hash] = next;
                }
            }

            return {
                testCaseHashes: _testCaseHashes,
                testResultsByTestCaseHash: _testResultsByTestCaseHash,
                ...rest,
            };
        };

        const currentTestRun = yield* tests.getOrCreateCurrentTestRun(
            testSuite.name,
        );
        yield* P.Effect.logDebug('currentTestRun');

        const hasResults = yield* tests.hasResults(currentTestRun);
        yield* P.Effect.logDebug('hasResults');

        const getFromRun = () =>
            PT.Test.all(testSuite, {concurrency: concurrency || 1}).pipe(
                P.Stream.tap(_ => tests.insertTestResult(_, testSuite.name)),
                PT.Test.runCollectRecord(currentTestRun),
                P.Effect.tap(P.Effect.logDebug('from run')),
                P.Effect.map(filter),
            );

        const getFromCache = () =>
            tests
                .getTestResultsStream(currentTestRun)
                .pipe(
                    PT.Test.runCollectRecord(currentTestRun),
                    P.Effect.tap(P.Effect.logDebug('from cache')),
                    P.Effect.map(filter),
                );

        const testRun: PT.Test.TestRunResults = yield* P.Effect.if(
            cached && hasResults,
            {onTrue: getFromCache, onFalse: getFromRun},
        );

        yield* P.Effect.logDebug('testRun');

        const previousTestRun = (yield* getPreviousTestRunResults(
            testSuite,
        )) as P.Option.Option<PT.Test.TestRunResults<I, O, T>>;

        yield* P.Effect.logDebug('previousTestRun');
        return {testRun, previousTestRun};
    }).pipe(P.Effect.withLogSpan('summarize'));

export const summarize = Command.make(
    'summarize',
    {labels, cached, orTags, andTags},
    ({labels, cached, orTags, andTags}) =>
        P.Effect.gen(function* () {
            const config = yield* Config;
            const {displayConfig} = config;
            const {testRun, previousTestRun} = yield* _sumarize({
                labels,
                orTags,
                andTags,
                cached,
                config,
            });

            if (testRun.testCaseHashes.length === 0) {
                yield* P.Console.log(
                    [
                        '┌─────────────────────────┐',
                        '│ NO TEST RESULTS VISIBLE │',
                        '└─────────────────────────┘',
                        '',
                        PT.Show.stats({testRun}),
                    ].join('\n'),
                );
                return;
            }

            yield* P.Console.log(
                [
                    PT.Show.summarize({
                        testRun,
                        previousTestRun,
                        displayConfig,
                    }),
                    '',
                    PT.Show.stats({testRun}),
                ].join('\n'),
            );
        }),
);
