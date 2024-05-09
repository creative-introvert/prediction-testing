import {isDeepStrictEqual} from 'node:util';
import {randomUUID} from 'node:crypto';

import type {ResultLengthMismatch, SqlError} from '@effect/sql/Error';
import {Command, Options} from '@effect/cli';
import * as PT from '@creative-introvert/prediction-testing';

import * as P from './prelude.js';
import {Config} from './Config.js';
import {getPreviousTestRunResults, shouldRun} from './common.js';

const LabelSchema = P.Schema.transform(
    P.Schema.String,
    P.Schema.Array(PT.Classify.LabelSchema),
    {
        decode: s => s.split(',') as readonly PT.Classify.Label[],
        encode: xs => xs.join(','),
    },
);

const labels = Options.text('labels').pipe(
    Options.withSchema(LabelSchema),
    Options.optional,
);

const createFilterLabel =
    (maybeLables: P.Option.Option<readonly PT.Classify.Label[]>) =>
    (tr: PT.Test.TestResult<unknown, unknown, unknown>) =>
        P.Option.match(maybeLables, {
            onNone: () => true,
            onSome: labels => labels.includes(tr.label),
        });

export const _sumarize = <I = unknown, O = unknown, T = unknown>({
    labels,
    shouldRun,
    config: {testSuite, displayConfig},
}: {
    labels: P.Option.Option<readonly PT.Classify.Label[]>;
    shouldRun: boolean;
    config: Config<I, O, T>;
}) =>
    P.Effect.gen(function* () {
        const filterLabel = createFilterLabel(labels);

        const currentTestRun =
            yield* PT.TestRepository.TestRepository.getOrCreateCurrentTestRun(
                testSuite.name,
            );
        yield* P.Effect.logDebug('currentTestRun');

        const hasResults =
            yield* PT.TestRepository.TestRepository.hasResults(currentTestRun);
        yield* P.Effect.logDebug('hasResults');

        const testRun: PT.Test.TestRunResults = yield* P.Effect.if(
            shouldRun || !hasResults,
            {
                onTrue: () =>
                    PT.Test.all(testSuite).pipe(
                        P.Effect.flatMap(
                            PT.Test.runCollectRecord(currentTestRun),
                        ),
                        P.Effect.tap(P.Effect.log('from run')),
                    ),
                onFalse: () =>
                    PT.TestRepository.TestRepository.getTestResultsStream(
                        currentTestRun,
                    ).pipe(
                        P.Effect.flatMap(
                            PT.Test.runCollectRecord(currentTestRun),
                        ),
                        P.Effect.tap(P.Effect.log('from cache')),
                    ),
            },
        );

        yield* P.Effect.logDebug('testRun');

        const previousTestRun = yield* getPreviousTestRunResults(testSuite);
        yield* P.Effect.logDebug('previousTestRun');
        return {testRun, previousTestRun};
    }).pipe(P.Effect.withLogSpan('summarize'));

export const summarize = Command.make(
    'summarize',
    {labels, shouldRun},
    ({labels, shouldRun}) =>
        P.Effect.gen(function* () {
            const config = yield* Config;
            const {testSuite, displayConfig, dbPath} = config;
            const {testRun, previousTestRun} = yield* _sumarize({
                labels,
                shouldRun,
                config,
            });

            if (testRun.testCaseHashes.length === 0) {
                yield* P.Console.log('Nothing to show.');
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
            console.timeLog('summarize', 'summarize');
            console.timeEnd('summarize');
        }),
);
