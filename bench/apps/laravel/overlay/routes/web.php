<?php

use App\Http\Controllers\BenchController;
use Illuminate\Support\Facades\Route;

// The five requests of bench/load: a document page, a feed, an insert, an update, a cross-organization report.
Route::withoutMiddleware('web')->group(function () {
    Route::get('/doc/{org}/{id}', [BenchController::class, 'read']);
    Route::get('/feed/{org}', [BenchController::class, 'feed']);
    Route::post('/doc/{org}', [BenchController::class, 'insert']);
    Route::put('/doc/{org}/{id}', [BenchController::class, 'update']);
    Route::get('/report', [BenchController::class, 'report']);
    Route::get('/', fn () => 'ok');
});
