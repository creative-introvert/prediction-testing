import {Command, Options} from '@effect/cli';
import * as PT from '@creative-introvert/prediction-testing';

import * as P from './prelude.js';

type Config<I = unknown, O = unknown, T = unknown> = {
    testSuite: PT.TestSuite<I, O, T>;
    dirPath: string;
    name: string;
};

export const Config = P.Context.GenericTag<Config>('Config');

export const makeConfigLayer = (config: Config) =>
    P.Layer.sync(Config, () => Config.of(config));

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
    (maybeLables: P.O.Option<readonly PT.Classify.Label[]>) =>
    (tr: PT.TestResult<unknown, unknown, unknown>) =>
        P.O.match(maybeLables, {
            onNone: () => true,
            onSome: labels => labels.includes(tr.label),
        });

const TestRunSchema = P.Schema.parseJson(PT.TestRunSchema);

const readPreviousTestRun = P.E.gen(function* (_) {
    const {testSuite, name, dirPath} = yield* _(Config);
    const fs = yield* _(P.FS.FileSystem);
    return yield* _(
        fs.readFileString(`${dirPath}/${name}.json`),
        P.E.flatMap(P.Schema.decodeUnknown(TestRunSchema)),
        P.E.option,
    );
});

const summarize = Command.make('summarize', {labels}, ({labels}) =>
    P.E.gen(function* (_) {
        const {testSuite} = yield* _(Config);

        const filterLabel = createFilterLabel(labels);
        const previousTestRun = yield* _(readPreviousTestRun);
        const testRun = yield* _(
            PT.testAll(testSuite).pipe(
                P.Stream.filter(filterLabel),
                PT.runFoldEffect,
            ),
        );
        yield* _(P.Console.log(PT.Show.summary({testRun, previousTestRun})));
        yield* _(P.Console.log(PT.Show.stats({testRun})));
    }),
);

// const diff = Command.make('diff', {}, () =>
//     P.E.gen(function* (_) {
//         const {testSuite} = yield* _(Config);
//
//         const testRun = yield* _(PT.testAll(testSuite).pipe(PT.runFoldEffect));
//         yield* _(P.Console.log(PT.Show.summary({testRun})));
//         yield* _(
//             P.Console.log(PT.Show.diff({testRun, diff: PT.diff({testRun})})),
//         );
//     }),
// );

// FIXME: Either sqlite backend, csv, or use line-delimited JSON.
const write = Command.make('write', {}, () =>
    P.E.gen(function* (_) {
        const {testSuite, dirPath, name} = yield* _(Config);
        const fs = yield* _(P.FS.FileSystem);

        yield* _(fs.makeDirectory(dirPath, {recursive: true}));

        const testRun = yield* _(PT.testAll(testSuite).pipe(PT.runFoldEffect));
        const filePath = `${dirPath}/${name}.json`;
        yield* _(
            fs.writeFileString(filePath, JSON.stringify(testRun, null, 2)),
        );
        yield* _(P.Console.log(`Wrote to "${filePath}"`));
    }),
);

const predictionTesting = Command.make('prediction-testing').pipe(
    Command.withSubcommands([summarize, write]),
);

const cli = Command.run(predictionTesting, {
    name: 'Prediction Testing',
    // FIXME
    version: 'v0.0.1',
});

export const run = <I, O, T>(config: Config<I, O, T>) =>
    P.E.suspend(() => cli(process.argv)).pipe(
        P.E.provide(
            P.NodeContext.layer.pipe(
                P.Layer.merge(makeConfigLayer(config as Config)),
            ),
        ),
        P.NodeRuntime.runMain,
    );