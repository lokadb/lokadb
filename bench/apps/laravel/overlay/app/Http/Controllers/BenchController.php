<?php

namespace App\Http\Controllers;

use App\Models\Document;
use App\Models\DocumentVersion;
use App\Models\Project;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

// What an ordinary Eloquent page does: the queries are the ORM's, nothing is tuned for LokaDB.
class BenchController extends Controller
{
    // a document page: the document, its project, how many versions it has
    public function read(int $org, int $id)
    {
        $d = Document::where('organization_id', $org)->where('id', $id)->firstOrFail();
        $p = Project::where('organization_id', $org)->where('id', $d->project_id)->first();
        return response()->json(['title' => $d->title, 'project' => $p?->name, 'versions' => $d->versions()->count()]);
    }

    // a feed: the latest documents of the organization with their projects (eager loading)
    public function feed(int $org)
    {
        $docs = Document::with('project')->where('organization_id', $org)->orderByDesc('id')->limit(20)->get();
        return response()->json($docs->map(fn ($d) => [$d->id, $d->title, $d->project?->name]));
    }

    // a new document and its first version in one transaction
    public function insert(Request $r, int $org)
    {
        $id = DB::transaction(function () use ($org, $r) {
            $p = Project::where('organization_id', $org)->firstOrFail();
            $d = Document::create(['organization_id' => $org, 'project_id' => $p->id, 'slug' => uniqid('b'), 'title' => $r->input('title', 'bench'), 'body' => 'b']);
            DocumentVersion::create(['document_id' => $d->id, 'content' => 'v1']);
            return $d->id;
        });
        return response()->json(['id' => $id], 201);
    }

    // edit a title: read the document, save it, in a transaction
    public function update(Request $r, int $org, int $id)
    {
        DB::transaction(function () use ($org, $id, $r) {
            $d = Document::where('organization_id', $org)->where('id', $id)->firstOrFail();
            $d->title = $r->input('title', 'bench');
            $d->save();
        });
        return response()->json(['ok' => true]);
    }

    // a report over all organizations: names no organization (the fanout policy decides)
    public function report()
    {
        return response()->json(DB::select('SELECT project_id, count(*) AS n FROM documents GROUP BY project_id ORDER BY n DESC LIMIT 10'));
    }
}
